package com.cs324a1.compute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.cs324a1.common.ComputeOperation;
import com.cs324a1.common.JobRequest;
import com.cs324a1.common.WorkResult;
import com.cs324a1.common.WorkUnit;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ResultAggregatorTest {

    @Test
    void aggregatesMaxResultsArrivingOutOfOrder() {
        JobRequest request = JobRequest.max("job-max", List.of(3, 9, -2, 7, 4));
        List<WorkUnit> units = WorkPartitioner.partition(request, 3);
        List<WorkResult> results = List.of(
                result(request, 2, 4),
                result(request, 0, 9),
                result(request, 1, 7));

        assertEquals(9L, ResultAggregator.aggregate(request, units, results));
    }

    @Test
    void aggregatesPrimeSumResults() {
        JobRequest request = JobRequest.primeSum("job-sum", 1, 20);
        List<WorkUnit> units = WorkPartitioner.partition(request, 2);

        assertEquals(77L, ResultAggregator.aggregate(request, units, List.of(
                result(request, 0, 17),
                result(request, 1, 60))));
    }

    @Test
    void aggregatesPrimeCountResults() {
        JobRequest request = JobRequest.primeCount("job-count", List.of(2, 4, 5, 8, 11));
        List<WorkUnit> units = WorkPartitioner.partition(request, 2);

        assertEquals(3L, ResultAggregator.aggregate(request, units, List.of(
                result(request, 0, 2),
                result(request, 1, 1))));
    }

    @Test
    void aggregatesEmptyPrimeCountAsZero() {
        JobRequest request = JobRequest.primeCount("job-empty", Collections.emptyList());

        assertEquals(0L, ResultAggregator.aggregate(request, List.of(), List.of()));
    }

    @Test
    void rejectsDuplicatePartitionResults() {
        JobRequest request = JobRequest.max("job", List.of(4, 9));
        List<WorkUnit> units = WorkPartitioner.partition(request, 2);
        WorkResult first = result(request, 0, 4);

        assertThrows(IllegalArgumentException.class,
                () -> ResultAggregator.aggregate(request, units,
                        List.of(first, first, result(request, 1, 9))));
    }

    @Test
    void rejectsMissingPartitionResults() {
        JobRequest request = JobRequest.primeSum("job", 1, 20);
        List<WorkUnit> units = WorkPartitioner.partition(request, 2);

        assertThrows(IllegalArgumentException.class,
                () -> ResultAggregator.aggregate(request, units,
                        List.of(result(request, 0, 17))));
    }

    @Test
    void rejectsResultFromWrongJob() {
        JobRequest request = JobRequest.max("expected-job", List.of(1));
        List<WorkUnit> units = WorkPartitioner.partition(request, 1);
        WorkResult wrongJob = new WorkResult(
                "other-job", 0, ComputeOperation.MAX, 1);

        assertThrows(IllegalArgumentException.class,
                () -> ResultAggregator.aggregate(request, units, List.of(wrongJob)));
    }

    @Test
    void rejectsResultWithWrongOperation() {
        JobRequest request = JobRequest.max("job", List.of(1));
        List<WorkUnit> units = WorkPartitioner.partition(request, 1);
        WorkResult wrongOperation = new WorkResult(
                "job", 0, ComputeOperation.PRIMECOUNT, 1);

        assertThrows(IllegalArgumentException.class,
                () -> ResultAggregator.aggregate(request, units, List.of(wrongOperation)));
    }

    @Test
    void rejectsUnexpectedPartitionResult() {
        JobRequest request = JobRequest.primeCount("job", List.of(2));
        List<WorkUnit> units = WorkPartitioner.partition(request, 1);

        assertThrows(IllegalArgumentException.class,
                () -> ResultAggregator.aggregate(request, units,
                        List.of(result(request, 1, 1))));
    }

    private static WorkResult result(JobRequest request, int partitionId, long value) {
        return new WorkResult(request.jobId(), partitionId, request.operation(), value);
    }
}
