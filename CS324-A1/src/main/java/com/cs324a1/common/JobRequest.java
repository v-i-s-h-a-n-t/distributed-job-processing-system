package com.cs324a1.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * Immutable top-level computation request submitted under a unique job ID.
 */
public record JobRequest(
        String jobId,
        ComputeOperation operation,
        List<Integer> numbers,
        Integer start,
        Integer end) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public JobRequest {
        validateJobId(jobId);
        if (operation == null) {
            throw new IllegalArgumentException("operation must not be null");
        }

        switch (operation) {
            case MAX -> {
                numbers = immutableNumbers(numbers, false);
                requireNoRange(start, end);
            }
            case PRIMECOUNT -> {
                numbers = immutableNumbers(numbers, true);
                requireNoRange(start, end);
            }
            case PRIMESUM -> {
                if (numbers != null) {
                    throw new IllegalArgumentException("PRIMESUM must not contain a numbers list");
                }
                validateRange(start, end);
            }
        }
    }

    public static JobRequest max(String jobId, List<Integer> numbers) {
        return new JobRequest(jobId, ComputeOperation.MAX, numbers, null, null);
    }

    public static JobRequest primeSum(String jobId, int start, int end) {
        return new JobRequest(jobId, ComputeOperation.PRIMESUM, null, start, end);
    }

    public static JobRequest primeCount(String jobId, List<Integer> numbers) {
        return new JobRequest(jobId, ComputeOperation.PRIMECOUNT, numbers, null, null);
    }

    private static void validateJobId(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId must not be blank");
        }
    }

    private static List<Integer> immutableNumbers(List<Integer> numbers, boolean allowEmpty) {
        if (numbers == null) {
            throw new IllegalArgumentException("numbers must not be null");
        }
        if (!allowEmpty && numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be empty");
        }
        for (Integer number : numbers) {
            if (number == null) {
                throw new IllegalArgumentException("numbers must not contain null values");
            }
        }
        return List.copyOf(numbers);
    }

    private static void requireNoRange(Integer start, Integer end) {
        if (start != null || end != null) {
            throw new IllegalArgumentException("list operations must not contain range bounds");
        }
    }

    private static void validateRange(Integer start, Integer end) {
        if (start == null || end == null) {
            throw new IllegalArgumentException("PRIMESUM requires both range bounds");
        }
        if (start > end) {
            throw new IllegalArgumentException("start must not be greater than end");
        }
    }
}
