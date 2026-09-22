package com.cs324a1.compute;

import com.cs324a1.common.ComputeOperation;
import com.cs324a1.common.JobRequest;
import com.cs324a1.common.WorkUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits validated jobs into deterministic, balanced work units.
 */
public final class WorkPartitioner {

    private WorkPartitioner() {
        // Utility class
    }

    public static List<WorkUnit> partition(JobRequest request, int requestedPartitions) {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        if (requestedPartitions <= 0) {
            throw new IllegalArgumentException("requestedPartitions must be positive");
        }

        return switch (request.operation()) {
            case MAX, PRIMECOUNT -> partitionNumbers(request, requestedPartitions);
            case PRIMESUM -> partitionRange(request, requestedPartitions);
        };
    }

    private static List<WorkUnit> partitionNumbers(
            JobRequest request, int requestedPartitions) {
        List<Integer> numbers = request.numbers();
        if (numbers.isEmpty()) {
            // Stage 2 permits an empty PRIMECOUNT request, whose final value is zero.
            return List.of();
        }

        int partitionCount = Math.min(requestedPartitions, numbers.size());
        int baseSize = numbers.size() / partitionCount;
        int remainder = numbers.size() % partitionCount;
        List<WorkUnit> partitions = new ArrayList<>(partitionCount);

        int offset = 0;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            int partitionSize = baseSize + (partitionId < remainder ? 1 : 0);
            List<Integer> values = numbers.subList(offset, offset + partitionSize);
            partitions.add(numberWorkUnit(request, partitionId, values));
            offset += partitionSize;
        }

        return List.copyOf(partitions);
    }

    private static WorkUnit numberWorkUnit(
            JobRequest request, int partitionId, List<Integer> values) {
        if (request.operation() == ComputeOperation.MAX) {
            return WorkUnit.max(request.jobId(), partitionId, values);
        }
        return WorkUnit.primeCount(request.jobId(), partitionId, values);
    }

    private static List<WorkUnit> partitionRange(
            JobRequest request, int requestedPartitions) {
        long valueCount = (long) request.end() - request.start() + 1L;
        int partitionCount = (int) Math.min((long) requestedPartitions, valueCount);
        long baseSize = valueCount / partitionCount;
        long remainder = valueCount % partitionCount;
        List<WorkUnit> partitions = new ArrayList<>(partitionCount);

        long nextStart = request.start();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            long partitionSize = baseSize + (partitionId < remainder ? 1L : 0L);
            long partitionEnd = nextStart + partitionSize - 1L;
            partitions.add(WorkUnit.primeSum(
                    request.jobId(), partitionId, (int) nextStart, (int) partitionEnd));
            nextStart = partitionEnd + 1L;
        }

        return List.copyOf(partitions);
    }
}
