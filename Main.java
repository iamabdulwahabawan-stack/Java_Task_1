import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

public class Main {

    // Global concurrent map to hold aggregated results
    private static final ConcurrentHashMap<String, Acc> GLOBAL = new ConcurrentHashMap<>();
    // Counters for logging
    private static final AtomicLong SKIPPED = new AtomicLong(0);
    // Total lines read (for logging purposes)
    private static final AtomicLong READ_LINES = new AtomicLong(0);

    // Decimal format for output numbers
    private static final DecimalFormat DF = new DecimalFormat("0.000000");

    // Accumulator class to hold sum and count
    private static final class Acc {
        final DoubleAdder sum = new DoubleAdder();
        final LongAdder count = new LongAdder();
        void add(double s, long c) { sum.add(s); count.add(c); }
    }

    public static void main(String[] args) {
        // Check command line arguments
        if (args.length != 1) {
            System.err.println("Usage: java ParallelCsvAggregator <csvPath>");
            System.exit(1);
        }
        // Assign the CSV file path from command line argument
        final Path csvPath = Paths.get(args[0]);
        // Check if the file is readable
        if (!Files.isReadable(csvPath)) {
            // print error and exit if not readable
            System.err.println("File not found or not readable: " + csvPath);
            System.exit(2);
        }
        // get the batch size from system property or get the default value
        final int batchSize = parseIntOrDefault(System.getProperty("batchSize"), 10000);
        // Calculate number of threads we need to use
        final int threads = Math.max(2, Runtime.getRuntime().availableProcessors()); 

        // Creating a fixed thread pool with daemon threads
        ExecutorService pool = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "agg-worker");
            t.setDaemon(true);
            return t;
        });
        // Create the list to hold submitted future tasks
        List<Future<?>> submitted = new ArrayList<>();

        // Try to read the CSV file using StandardCharsets.UTF_8
        try (BufferedReader br = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
            // Creating a header line variable
            String headerLine;
            // Reading the header line and skip the empty lines
            do { headerLine = br.readLine(); } while (headerLine != null && headerLine.trim().isEmpty());

            // if the header line is null, it means the file might be empty
            if (headerLine == null) {
                System.out.println("userId,sum,avg");
                return;
            }
            // Paring the header line to get the column names
            List<String> header = parseCsvLine(headerLine);
            // Trying to get the get the indexing of userId and amount columns before we cannot hard code the sequence
            int userIdx = findColumnIndex(header, "userId");
            int amountIdx = findColumnIndex(header, "amount");
            
            // if either the userid or amount column is not found, we simply cannot proceed
            if (userIdx < 0 || amountIdx < 0) {
                System.err.println("Could not find required columns 'userId' and 'amount' in header.");
                System.exit(3);
            }

            // Creating the list of string to hold the batch of lines
            List<String> batch = new ArrayList<>(batchSize);
            String line;
            // Reading the file line by line
            while ((line = br.readLine()) != null) {
                READ_LINES.incrementAndGet();
                // check if the line is blank, simply skip that line
                if (line.isBlank()) continue;
                // Add the line to the current batch
                batch.add(line);

                // Check if the batch size is reached, if we have reached the batch size, we submit the batch for processing
                if (batch.size() >= batchSize) {
                    // Submit the batch and clear it for the next batch
                    submitted.add(submitBatch(new ArrayList<>(batch), userIdx, amountIdx, header, pool));
                    batch.clear();
                }
            }
            // Check if the batch list is not empty, 
            if (!batch.isEmpty()) {
                // We simply submit the remaining lines as the last batch
                submitted.add(submitBatch(new ArrayList<>(batch), userIdx, amountIdx, header, pool));
            }

        } catch (IOException e) {
            System.err.println("I/O error: " + e.getMessage());
            System.exit(4);
        }

        // Waiting for all submitted task to complete
        for (Future<?> f : submitted) {
            try {
                f.get(); // propagate exceptions
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                System.err.println("Interrupted while waiting for tasks.");
                break;
            } catch (ExecutionException ee) {
                System.err.println("Worker failed: " + ee.getCause());
            }
        }
        // Shutdowing the thread pool
        pool.shutdown();
        try {
            // Check if all tasks are finished within 60 seconds, otherwise force shutdown
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException ie) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Output results as CSV sorted by userId
        System.out.println("userId,sum,avg");
        GLOBAL.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> {
                    String user = e.getKey();
                    double sum = e.getValue().sum.sum();
                    long cnt = e.getValue().count.sum();
                    double avg = (cnt == 0) ? 0.0 : sum / cnt;
                    System.out.println(user + "," + DF.format(sum) + "," + DF.format(avg));
                });

        // Log skipped info to stderr (not stdout)
        System.err.println("Skipped lines: " + SKIPPED.get() + " (out of " + READ_LINES.get() + " data lines)");
    }

    // Submit a batch of lines for processing
    private static Future<?> submitBatch(
            List<String> batch,
            int userIdx,
            int amountIdx,
            List<String> header,
            ExecutorService pool
    ) {
        // Precompute max index and lowercased header for duplicate detection
        final int maxIndex = Math.max(userIdx, amountIdx);
        // Lowercase trimmed header for duplicate detection
        final List<String> headerLower = header.stream().map(s -> s == null ? "" : s.trim().toLowerCase(Locale.ROOT)).toList();

        // Submit the batch processing task to the thread pool
        return pool.submit(() -> {
            // Local map to hold intermediate results for this batch
            Map<String, double[]> local = new HashMap<>(1024);
            long skippedLocal = 0;
            // Process each line in the batch
            for (String line : batch) {
                List<String> cols = parseCsvLine(line);

                // Skip lines with insufficient columns
                if (cols.size() <= maxIndex) { skippedLocal++; continue; }

                // Handle duplicate header lines inside the file
                if (isDuplicateHeaderRow(cols, headerLower, userIdx, amountIdx)) {
                    continue;
                }

                // Extract userId and amount values
                String user = safeGet(cols, userIdx);
                String amountStr = safeGet(cols, amountIdx);

                if (user == null || user.isBlank()) { skippedLocal++; continue; }
                double amount;
                try {
                    amount = Double.parseDouble(amountStr);
                } catch (Exception ex) {
                    skippedLocal++; continue;
                }

                double[] acc = local.computeIfAbsent(user, k -> new double[2]); // [sum, count]
                acc[0] += amount;
                acc[1] += 1.0;
            }

            // Merge local -> global
            for (Map.Entry<String, double[]> e : local.entrySet()) {
                final String user = e.getKey();
                final double sum = e.getValue()[0];
                final long cnt = (long) e.getValue()[1];
                GLOBAL.computeIfAbsent(user, k -> new Acc()).add(sum, cnt);
            }

            if (skippedLocal > 0) SKIPPED.addAndGet(skippedLocal);
        });
    }

    // Check if a row is a duplicate header row
    private static boolean isDuplicateHeaderRow(List<String> row, List<String> headerLower, int userIdx, int amountIdx) {
        // Quick path: if the row has different size than header, it's probably data.
        if (row.size() < Math.max(headerLower.size(), Math.max(userIdx, amountIdx) + 1)) return false;
        // if the row size is larger than header size, we cannot be a duplicate header
        String u = row.get(userIdx) == null ? "" : row.get(userIdx).trim().toLowerCase(Locale.ROOT);
        String a = row.get(amountIdx) == null ? "" : row.get(amountIdx).trim().toLowerCase(Locale.ROOT);
        return "userid".equals(u) && "amount".equals(a);
    }
    // Safely get a column value by index, returning null if out of bounds
    private static String safeGet(List<String> cols, int idx) {
        if (idx < 0 || idx >= cols.size()) return null;
        return cols.get(idx);
    }
    // Find the index of a column in the header, case-insensitive
    private static int findColumnIndex(List<String> header, String expected) {
        for (int i = 0; i < header.size(); i++) {
            String h = header.get(i);
            if (h != null && h.trim().equalsIgnoreCase(expected)) return i;
        }
        return -1;
    }

    // Parse a CSV line into columns, handling quotes and commas
    private static List<String> parseCsvLine(String line) {
        ArrayList<String> out = new ArrayList<>();
        if (line == null) return out;
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        cur.append('"'); 
                        i++;              
                    } else {
                        inQuotes = false; 
                    }
                } else {
                    cur.append(c);
                }
            } else { // not in quotes
                if (c == ',') {
                    out.add(cur.toString());
                    cur.setLength(0);
                } else if (c == '"') {
                    inQuotes = true;
                } else {
                    cur.append(c);
                }
            }
        }
        out.add(cur.toString());
        return out;
    }

    private static int parseIntOrDefault(String s, int def) {
        if (s == null) return def;
        try { return Integer.parseInt(s); } catch (Exception e) { return def; }
    }
}
