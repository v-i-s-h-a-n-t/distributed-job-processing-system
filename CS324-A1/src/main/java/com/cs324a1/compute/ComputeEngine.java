package com.cs324a1.compute;

import java.util.List;

/**
 * Pure implementations of the computations supported by the job system.
 */
public final class ComputeEngine {

    private ComputeEngine() {
        // Utility class
    }

    /**
     * Returns the largest value in an unsorted list.
     *
     * @throws IllegalArgumentException if the list is null, empty, or contains null
     */
    public static int max(List<Integer> numbers) {
        validateNumbers(numbers, false);

        int maximum = numbers.get(0);
        for (int number : numbers) {
            if (number > maximum) {
                maximum = number;
            }
        }
        return maximum;
    }

    /**
     * Returns the sum of all prime numbers in the inclusive range.
     *
     * @throws IllegalArgumentException if start is greater than end
     */
    public static long primeSum(int start, int end) {
        if (start > end) {
            throw new IllegalArgumentException("start must not be greater than end");
        }

        long sum = 0;
        for (long candidate = Math.max(2L, start); candidate <= end; candidate++) {
            if (isPrime((int) candidate)) {
                sum += candidate;
            }
        }
        return sum;
    }

    /**
     * Counts prime values in an unsorted list. Duplicate primes are counted
     * separately because each list entry is an input value.
     *
     * @throws IllegalArgumentException if the list is null or contains null
     */
    public static int primeCount(List<Integer> numbers) {
        validateNumbers(numbers, true);

        int count = 0;
        for (int number : numbers) {
            if (isPrime(number)) {
                count++;
            }
        }
        return count;
    }

    private static void validateNumbers(List<Integer> numbers, boolean allowEmpty) {
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
    }

    private static boolean isPrime(int number) {
        if (number < 2) {
            return false;
        }
        if (number == 2) {
            return true;
        }
        if (number % 2 == 0) {
            return false;
        }

        for (int divisor = 3; divisor <= number / divisor; divisor += 2) {
            if (number % divisor == 0) {
                return false;
            }
        }
        return true;
    }
}
