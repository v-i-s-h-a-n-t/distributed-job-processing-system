package com.cs324a1.compute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ComputeEngineTest {

    @Test
    void maxReturnsLargestValueFromUnsortedList() {
        assertEquals(12, ComputeEngine.max(List.of(4, 12, 1, 9, 3)));
    }

    @Test
    void maxHandlesAllNegativeValues() {
        assertEquals(-2, ComputeEngine.max(List.of(-8, -2, -11, -5)));
    }

    @Test
    void maxHandlesSingleValue() {
        assertEquals(-7, ComputeEngine.max(List.of(-7)));
    }

    @Test
    void maxRejectsNullOrEmptyInput() {
        assertThrows(IllegalArgumentException.class, () -> ComputeEngine.max(null));
        assertThrows(IllegalArgumentException.class, () -> ComputeEngine.max(Collections.emptyList()));
    }

    @Test
    void maxRejectsNullValues() {
        assertThrows(IllegalArgumentException.class,
                () -> ComputeEngine.max(Arrays.asList(3, null, 5)));
    }

    @Test
    void primeSumIncludesBothRangeEndpoints() {
        assertEquals(17L, ComputeEngine.primeSum(2, 10));
    }

    @Test
    void primeSumReturnsZeroWhenRangeContainsNoPrimes() {
        assertEquals(0L, ComputeEngine.primeSum(14, 16));
        assertEquals(0L, ComputeEngine.primeSum(-10, 1));
    }

    @Test
    void primeSumHandlesSinglePrimeValue() {
        assertEquals(2L, ComputeEngine.primeSum(2, 2));
    }

    @Test
    void primeSumHandlesLargestIntegerWithoutLoopOverflow() {
        assertEquals(2_147_483_647L,
                ComputeEngine.primeSum(Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    @Test
    void primeSumRejectsReversedRange() {
        assertThrows(IllegalArgumentException.class, () -> ComputeEngine.primeSum(10, 2));
    }

    @Test
    void primeCountCountsPrimeValuesInUnsortedList() {
        assertEquals(3, ComputeEngine.primeCount(List.of(11, 4, 2, 9, 5, 1)));
    }

    @Test
    void primeCountTreatsValuesBelowTwoAsNonPrime() {
        assertEquals(0, ComputeEngine.primeCount(List.of(-7, -1, 0, 1)));
    }

    @Test
    void primeCountCountsDuplicatePrimesSeparately() {
        assertEquals(4, ComputeEngine.primeCount(List.of(2, 2, 4, 5, 5)));
    }

    @Test
    void primeCountAcceptsEmptyList() {
        assertEquals(0, ComputeEngine.primeCount(Collections.emptyList()));
    }

    @Test
    void primeCountRejectsNullInputOrNullValues() {
        assertThrows(IllegalArgumentException.class, () -> ComputeEngine.primeCount(null));
        assertThrows(IllegalArgumentException.class,
                () -> ComputeEngine.primeCount(Arrays.asList(2, null, 3)));
    }
}
