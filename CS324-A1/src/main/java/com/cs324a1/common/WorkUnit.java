package com.cs324a1.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * Immutable partition of a job that can be sent to one worker.
 */
public record WorkUnit(
        String jobId,
        int partitionId,
        ComputeOperation operation,
        List<Integer> numbers,
        Integer start,
        Integer end) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public WorkUnit {
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must not be negative");
        }

        JobRequest validatedPayload = new JobRequest(jobId, operation, numbers, start, end);
        jobId = validatedPayload.jobId();
        operation = validatedPayload.operation();
        numbers = validatedPayload.numbers();
        start = validatedPayload.start();
        end = validatedPayload.end();

        if (operation == ComputeOperation.PRIMECOUNT && numbers.isEmpty()) {
            throw new IllegalArgumentException("a PRIMECOUNT work unit must not be empty");
        }
    }

    public static WorkUnit max(String jobId, int partitionId, List<Integer> numbers) {
        return new WorkUnit(jobId, partitionId, ComputeOperation.MAX, numbers, null, null);
    }

    public static WorkUnit primeSum(String jobId, int partitionId, int start, int end) {
        return new WorkUnit(jobId, partitionId, ComputeOperation.PRIMESUM, null, start, end);
    }

    public static WorkUnit primeCount(String jobId, int partitionId, List<Integer> numbers) {
        return new WorkUnit(jobId, partitionId, ComputeOperation.PRIMECOUNT, numbers, null, null);
    }
}
