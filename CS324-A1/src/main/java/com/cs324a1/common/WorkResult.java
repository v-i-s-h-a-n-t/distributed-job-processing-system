package com.cs324a1.common;

import java.io.Serial;
import java.io.Serializable;

/**
 * Immutable scalar result produced for one work-unit partition.
 */
public record WorkResult(
        String jobId,
        int partitionId,
        ComputeOperation operation,
        long value) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public WorkResult {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId must not be blank");
        }
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must not be negative");
        }
        if (operation == null) {
            throw new IllegalArgumentException("operation must not be null");
        }
        if ((operation == ComputeOperation.PRIMESUM
                || operation == ComputeOperation.PRIMECOUNT) && value < 0) {
            throw new IllegalArgumentException(operation + " result must not be negative");
        }
    }
}
