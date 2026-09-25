package com.cs324a1.client;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses manual text entry and CSV files into job inputs.
 *
 * <p>List jobs (MAX, PRIMECOUNT) accept comma/whitespace/newline separated
 * integers, with an optional single header row (e.g. {@code value}).
 * Range jobs (PRIMESUM) need {@code start,end}.
 */
public final class JobParser {

    private JobParser() {
    }

    /** Parses manual text like "2, 4 5\n11" into integers. */
    public static List<Integer> parseNumberList(String text) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("numbers must not be empty");
        }
        String[] tokens = text.trim().split("[,;\\s]+");
        List<Integer> numbers = new ArrayList<>(tokens.length);
        for (String token : tokens) {
            if (token.isBlank()) {
                continue;
            }
            try {
                numbers.add(Integer.parseInt(token.trim()));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid integer: '" + token + "'", e);
            }
        }
        if (numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be empty");
        }
        return List.copyOf(numbers);
    }

    /**
     * Loads a numbers CSV. Accepts single-column (one int per line, optional
     * header) or single/multiple comma-separated values per line.
     */
    public static List<Integer> loadNumberList(File file) throws IOException {
        List<Integer> numbers = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            boolean firstLine = true;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                // Skip a single header row like "value" or "numbers".
                if (firstLine && line.matches("(?i)[a-z_]+.*") && !line.matches("-?\\d.*")) {
                    firstLine = false;
                    // If the header line itself contains digits, fall through;
                    // otherwise skip it.
                    if (!line.matches(".*\\d.*")) {
                        continue;
                    }
                }
                firstLine = false;
                for (String token : line.split("[,;\\s]+")) {
                    if (token.isBlank()) {
                        continue;
                    }
                    try {
                        numbers.add(Integer.parseInt(token.trim()));
                    } catch (NumberFormatException e) {
                        throw new IOException("invalid integer '" + token + "' in " + file.getName(), e);
                    }
                }
            }
        }
        if (numbers.isEmpty()) {
            throw new IOException("no numbers found in " + file.getName());
        }
        return List.copyOf(numbers);
    }

    /** Loads a PRIMESUM range. Accepts "start,end" (single row, header optional). */
    public static int[] loadPrimeSumRange(File file) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                if (line.matches("(?i)[a-z_,;\\s]+") && !line.matches(".*\\d.*")) {
                    continue; // header like "start,end"
                }
                String[] tokens = line.split("[,;\\s]+");
                if (tokens.length < 2) {
                    throw new IOException("expected 'start,end' in " + file.getName() + " but got: " + line);
                }
                try {
                    int start = Integer.parseInt(tokens[0].trim());
                    int end = Integer.parseInt(tokens[1].trim());
                    if (start > end) {
                        throw new IOException("start must not be greater than end in " + file.getName());
                    }
                    return new int[]{start, end};
                } catch (NumberFormatException e) {
                    throw new IOException("invalid range '" + line + "' in " + file.getName(), e);
                }
            }
        }
        throw new IOException("no start,end range found in " + file.getName());
    }
}
