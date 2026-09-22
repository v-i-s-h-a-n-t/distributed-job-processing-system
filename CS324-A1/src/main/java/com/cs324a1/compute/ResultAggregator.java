package com.cs324a1.compute;

import com.cs324a1.common.ComputeOperation;
import com.cs324a1.common.JobRequest;
import com.cs324a1.common.WorkResult;
import com.cs324a1.common.WorkUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates and combines the partial results produced for a partitioned job.
 */
public final class ResultAggregator {

    private ResultAggregator() {
        // Utility class
    }

    public static long aggregate(
            JobRequest request,
            List<WorkUnit> expectedWorkUnits,
            List<WorkResult> results) {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        if (expectedWorkUnits == null) {
            throw new IllegalArgumentException("expectedWorkUnits must not be null");
        }
        if (results == null) {
            throw new IllegalArgumentException("results must not be null");
        }

        Map<Integer, WorkUnit> expectedByPartition = validateExpectedWork(request, expectedWorkUnits);
        if (expectedByPartition.isEmpty()) {
            if (request.operation() != ComputeOperation.PRIMECOUNT
                    || !request.numbers().isEmpty()) {
                throw new IllegalArgumentException("a non-empty job must have work units");
            }
            if (!results.isEmpty()) {
                throw new IllegalArgumentException("an empty PRIMECOUNT job must not have results");
            }
            return 0L;
        }

        Set<Integer> completedPartitions = new HashSet<>();
        long aggregate = request.operation() == ComputeOperation.MAX ? Long.MIN_VALUE : 0L;

        for (WorkResult result : results) {
            validateResult(request, expectedByPartition, completedPartitions, result);
            aggregate = combine(request.operation(), aggregate, result.value());
        }

        if (completedPartitions.size() != expectedByPartition.size()) {
            throw new IllegalArgumentException("one or more partition results are missing");
        }
        return aggregate;
    }

    private static Map<Integer, WorkUnit> validateExpectedWork(
            JobRequest request, List<WorkUnit> expectedWorkUnits) {
        Map<Integer, WorkUnit> expectedByPartition = new HashMap<>();
        for (WorkUnit unit : expectedWorkUnits) {
            if (unit == null) {
                throw new IllegalArgumentException("expectedWorkUnits must not contain null");
            }
            if (!request.jobId().equals(unit.jobId())) {
                throw new IllegalArgumentException("work unit belongs to a different job");
            }
            if (request.operation() != unit.operation()) {
                throw new IllegalArgumentException("work unit uses a different operation");
            }
            if (expectedByPartition.put(unit.partitionId(), unit) != null) {
                throw new IllegalArgumentException("duplicate expected partition ID");
            }
        }
        return expectedByPartition;
    }

    private static void validateResult(
            JobRequest request,
            Map<Integer, WorkUnit> expectedByPartition,
            Set<Integer> completedPartitions,
            WorkResult result) {
        if (result == null) {
            throw new IllegalArgumentException("results must not contain null");
        }
        if (!request.jobId().equals(result.jobId())) {
            throw new IllegalArgumentException("result belongs to a different job");
        }
        if (request.operation() != result.operation()) {
            throw new IllegalArgumentException("result uses a different operation");
        }
        if (!expectedByPartition.containsKey(result.partitionId())) {
            throw new IllegalArgumentException("result has an unexpected partition ID");
        }
        if (!completedPartitions.add(result.partitionId())) {
            throw new IllegalArgumentException("duplicate partition result");
        }
    }

    private static long combine(ComputeOperation operation, long current, long value) {
        return switch (operation) {
            case MAX -> Math.max(current, value);
            case PRIMESUM, PRIMECOUNT -> Math.addExact(current, value);
        };
    }
}
