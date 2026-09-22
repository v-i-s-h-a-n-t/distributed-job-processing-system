package com.cs324a1.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ComputeContractsTest {

    @Test
    void operationListsExactlyTheSupportedComputations() {
        assertArrayEquals(
                new ComputeOperation[] {
                    ComputeOperation.MAX,
                    ComputeOperation.PRIMESUM,
                    ComputeOperation.PRIMECOUNT
                },
                ComputeOperation.values());
    }

    @Test
    void jobRequestFactoriesCreateOperationSpecificPayloads() {
        JobRequest max = JobRequest.max("job-max", List.of(8, 3, 5));
        assertEquals(ComputeOperation.MAX, max.operation());
        assertEquals(List.of(8, 3, 5), max.numbers());
        assertNull(max.start());
        assertNull(max.end());

        JobRequest primeSum = JobRequest.primeSum("job-sum", -5, 20);
        assertEquals(ComputeOperation.PRIMESUM, primeSum.operation());
        assertNull(primeSum.numbers());
        assertEquals(-5, primeSum.start());
        assertEquals(20, primeSum.end());

        JobRequest primeCount = JobRequest.primeCount("job-count", List.of(2, 4, 5));
        assertEquals(ComputeOperation.PRIMECOUNT, primeCount.operation());
        assertEquals(List.of(2, 4, 5), primeCount.numbers());
    }

    @Test
    void jobRequestDefensivelyCopiesNumbers() {
        List<Integer> source = new ArrayList<>(List.of(4, 9, 2));
        JobRequest request = JobRequest.max("job-1", source);

        source.set(0, 100);

        assertEquals(List.of(4, 9, 2), request.numbers());
        assertThrows(UnsupportedOperationException.class, () -> request.numbers().add(7));
    }

    @Test
    void jobRequestRejectsInvalidIdentifiersAndOperations() {
        assertThrows(IllegalArgumentException.class,
                () -> JobRequest.max(" ", List.of(1)));
        assertThrows(IllegalArgumentException.class,
                () -> new JobRequest("job", null, List.of(1), null, null));
    }

    @Test
    void jobRequestRejectsInvalidListPayloads() {
        assertThrows(IllegalArgumentException.class, () -> JobRequest.max("job", null));
        assertThrows(IllegalArgumentException.class,
                () -> JobRequest.max("job", Collections.emptyList()));
        assertThrows(IllegalArgumentException.class,
                () -> JobRequest.primeCount("job", Arrays.asList(2, null, 3)));
    }

    @Test
    void primeCountJobMayBeEmpty() {
        assertEquals(Collections.emptyList(),
                JobRequest.primeCount("job", Collections.emptyList()).numbers());
    }

    @Test
    void jobRequestRejectsInvalidRangeOrMismatchedPayload() {
        assertThrows(IllegalArgumentException.class,
                () -> JobRequest.primeSum("job", 10, 2));
        assertThrows(IllegalArgumentException.class,
                () -> new JobRequest("job", ComputeOperation.PRIMESUM,
                        List.of(2, 3), 2, 10));
        assertThrows(IllegalArgumentException.class,
                () -> new JobRequest("job", ComputeOperation.MAX,
                        List.of(1), 1, 10));
    }

    @Test
    void workUnitCarriesJobAndPartitionIdentity() {
        WorkUnit unit = WorkUnit.primeSum("job-7", 3, 100, 150);

        assertEquals("job-7", unit.jobId());
        assertEquals(3, unit.partitionId());
        assertEquals(ComputeOperation.PRIMESUM, unit.operation());
        assertEquals(100, unit.start());
        assertEquals(150, unit.end());
    }

    @Test
    void workUnitRejectsInvalidPartitionOrEmptyListPartition() {
        assertThrows(IllegalArgumentException.class,
                () -> WorkUnit.max("job", -1, List.of(1)));
        assertThrows(IllegalArgumentException.class,
                () -> WorkUnit.primeCount("job", 0, Collections.emptyList()));
    }

    @Test
    void workResultValidatesIdentityAndOperationSpecificValue() {
        assertThrows(IllegalArgumentException.class,
                () -> new WorkResult("", 0, ComputeOperation.MAX, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new WorkResult("job", -1, ComputeOperation.MAX, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new WorkResult("job", 0, null, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new WorkResult("job", 0, ComputeOperation.PRIMECOUNT, -1));

        assertEquals(-12L,
                new WorkResult("job", 0, ComputeOperation.MAX, -12).value());
    }

    @Test
    void contractsSurviveJavaSerializationRoundTrip() throws Exception {
        List<Serializable> contracts = List.of(
                ComputeOperation.PRIMECOUNT,
                JobRequest.max("job-max", List.of(-3, -8)),
                JobRequest.primeSum("job-sum", 1, 50),
                JobRequest.primeCount("job-count", List.of(2, 2, 4)),
                WorkUnit.max("job-max", 0, List.of(-3)),
                WorkUnit.primeSum("job-sum", 1, 25, 50),
                WorkUnit.primeCount("job-count", 2, List.of(2, 4)),
                new WorkResult("job-count", 2, ComputeOperation.PRIMECOUNT, 1));

        for (Serializable contract : contracts) {
            assertEquals(contract, serializationRoundTrip(contract));
        }
    }

    private static Object serializationRoundTrip(Serializable value)
            throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }

        try (ObjectInputStream input = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            return input.readObject();
        }
    }
}
