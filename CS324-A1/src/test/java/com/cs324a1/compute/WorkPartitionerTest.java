package com.cs324a1.compute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cs324a1.common.JobRequest;
import com.cs324a1.common.WorkUnit;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkPartitionerTest {

    @Test
    void partitionsListEvenlyAndPreservesOrder() {
        JobRequest request = JobRequest.max("job", List.of(8, 3, 6, 1, 9, 2));

        List<WorkUnit> units = WorkPartitioner.partition(request, 3);

        assertEquals(3, units.size());
        assertEquals(List.of(8, 3), units.get(0).numbers());
        assertEquals(List.of(6, 1), units.get(1).numbers());
        assertEquals(List.of(9, 2), units.get(2).numbers());
    }

    @Test
    void assignsUnevenListRemainderToEarliestPartitions() {
        JobRequest request = JobRequest.primeCount(
                "job", List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        List<WorkUnit> units = WorkPartitioner.partition(request, 3);

        assertEquals(List.of(0, 1, 2, 3), units.get(0).numbers());
        assertEquals(List.of(4, 5, 6), units.get(1).numbers());
        assertEquals(List.of(7, 8, 9), units.get(2).numbers());
    }

    @Test
    void createsNoMoreListPartitionsThanValues() {
        JobRequest request = JobRequest.max("job", List.of(12, 4));

        List<WorkUnit> units = WorkPartitioner.partition(request, 5);

        assertEquals(2, units.size());
        assertEquals(List.of(12), units.get(0).numbers());
        assertEquals(List.of(4), units.get(1).numbers());
    }

    @Test
    void partitionsInclusivePrimeSumRangeEvenly() {
        JobRequest request = JobRequest.primeSum("job", 1, 1000);

        List<WorkUnit> units = WorkPartitioner.partition(request, 4);

        assertRange(units.get(0), 1, 250);
        assertRange(units.get(1), 251, 500);
        assertRange(units.get(2), 501, 750);
        assertRange(units.get(3), 751, 1000);
    }

    @Test
    void balancesUnevenRangeWithoutGapsOrOverlaps() {
        JobRequest request = JobRequest.primeSum("job", -2, 7);

        List<WorkUnit> units = WorkPartitioner.partition(request, 4);

        assertEquals(-2, units.get(0).start());
        assertEquals(7, units.get(units.size() - 1).end());

        long smallestSize = Long.MAX_VALUE;
        long largestSize = Long.MIN_VALUE;
        for (int index = 0; index < units.size(); index++) {
            WorkUnit unit = units.get(index);
            long size = (long) unit.end() - unit.start() + 1L;
            smallestSize = Math.min(smallestSize, size);
            largestSize = Math.max(largestSize, size);
            assertEquals(index, unit.partitionId());
            if (index > 0) {
                assertEquals((long) units.get(index - 1).end() + 1L, unit.start().longValue());
            }
        }
        assertTrue(largestSize - smallestSize <= 1L);
    }

    @Test
    void createsOnePartitionForSingleValueRange() {
        JobRequest request = JobRequest.primeSum("job", 17, 17);

        List<WorkUnit> units = WorkPartitioner.partition(request, 8);

        assertEquals(1, units.size());
        assertRange(units.get(0), 17, 17);
    }

    @Test
    void emptyPrimeCountProducesNoInvalidWorkUnit() {
        JobRequest request = JobRequest.primeCount("job", Collections.emptyList());

        assertEquals(Collections.emptyList(), WorkPartitioner.partition(request, 3));
    }

    @Test
    void rejectsInvalidPartitionRequests() {
        JobRequest request = JobRequest.max("job", List.of(1));

        assertThrows(IllegalArgumentException.class, () -> WorkPartitioner.partition(null, 1));
        assertThrows(IllegalArgumentException.class, () -> WorkPartitioner.partition(request, 0));
        assertThrows(IllegalArgumentException.class, () -> WorkPartitioner.partition(request, -1));
    }

    private static void assertRange(WorkUnit unit, int expectedStart, int expectedEnd) {
        assertEquals(expectedStart, unit.start());
        assertEquals(expectedEnd, unit.end());
    }
}
