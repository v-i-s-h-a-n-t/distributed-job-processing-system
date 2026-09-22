package com.cs324a1.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cs324a1.common.JobRequest;
import com.cs324a1.common.WorkResult;
import com.cs324a1.common.WorkUnit;
import com.cs324a1.common.WorkerInterface;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CoordinatorDispatchTest {

    private final List<TestWorkerNode> workers = new ArrayList<>();

    @AfterEach
    void cleanUpWorkers() {
        for (TestWorkerNode worker : workers) {
            worker.shutdownWorkerExecutors();
            try {
                UnicastRemoteObject.unexportObject(worker, true);
            } catch (Exception ignored) {
                // already unexported
            }
        }
    }

    @Test
    void distributesBalancedMaxPartitionsAndAggregatesResult() throws Exception {
        TestWorkerNode coordinator = coordinator(10);
        TestWorkerNode second = worker(20);
        TestWorkerNode third = worker(30);
        coordinator.setDiscoveredWorkers(List.of(stub(third), coordinator, stub(second)));

        long result = coordinator.submitJob(JobRequest.max(
                "max-job", List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)));

        assertEquals(9L, result);
        assertEquals(List.of(0, 1, 2, 3), coordinator.lastWorkUnit.numbers());
        assertEquals(List.of(4, 5, 6), second.lastWorkUnit.numbers());
        assertEquals(List.of(7, 8, 9), third.lastWorkUnit.numbers());
        assertEquals(0, coordinator.lastWorkUnit.partitionId());
        assertEquals(1, second.lastWorkUnit.partitionId());
        assertEquals(2, third.lastWorkUnit.partitionId());
    }

    @Test
    void distributesAndAggregatesPrimeSum() throws Exception {
        TestWorkerNode coordinator = coordinator(10);
        TestWorkerNode second = worker(20);
        coordinator.setDiscoveredWorkers(List.of(coordinator, stub(second)));

        assertEquals(77L, coordinator.submitJob(JobRequest.primeSum("sum-job", 1, 20)));
        assertRange(coordinator.lastWorkUnit, 1, 10);
        assertRange(second.lastWorkUnit, 11, 20);
    }

    @Test
    void distributesAndAggregatesPrimeCount() throws Exception {
        TestWorkerNode coordinator = coordinator(10);
        TestWorkerNode second = worker(20);
        coordinator.setDiscoveredWorkers(List.of(coordinator, stub(second)));

        assertEquals(4L, coordinator.submitJob(JobRequest.primeCount(
                "count-job", List.of(2, 4, 5, 8, 11, 13))));
    }

    @Test
    void nonCoordinatorForwardsToKnownCoordinator() throws Exception {
        TestWorkerNode coordinator = coordinator(10);
        TestWorkerNode follower = worker(20);
        coordinator.setDiscoveredWorkers(List.of(coordinator));
        follower.propagateCoordinator(
                "forwarding-coordinator", 10, stub(coordinator), null);

        long result = follower.submitJob(JobRequest.max("forwarded-job", List.of(3, 12, 5)));

        assertEquals(12L, result);
        assertEquals("forwarded-job", coordinator.lastWorkUnit.jobId());
        assertEquals(0, follower.executionCount);
    }

    @Test
    void emptyPrimeCountReturnsZeroWithoutDispatch() throws Exception {
        TestWorkerNode coordinator = coordinator(10);
        TestWorkerNode second = worker(20);
        coordinator.setDiscoveredWorkers(List.of(coordinator, stub(second)));

        long result = coordinator.submitJob(
                JobRequest.primeCount("empty-job", Collections.emptyList()));

        assertEquals(0L, result);
        assertEquals(0, coordinator.executionCount);
        assertEquals(0, second.executionCount);
    }

    @Test
    void dispatchesPartitionsConcurrently() throws Exception {
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        TestWorkerNode coordinator = coordinator(10);
        TestWorkerNode second = worker(20);
        coordinator.blockWith(bothStarted, release);
        second.blockWith(bothStarted, release);
        coordinator.setDiscoveredWorkers(List.of(coordinator, stub(second)));
        ExecutorService caller = Executors.newSingleThreadExecutor();

        try {
            Future<Long> result = caller.submit(() -> coordinator.submitJob(
                    JobRequest.max("concurrent-job", List.of(1, 8, 3, 9))));

            assertTrue(bothStarted.await(2, TimeUnit.SECONDS),
                    "both workers should begin before either partition is released");
            release.countDown();
            assertEquals(9L, result.get(2, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            caller.shutdownNow();
        }
    }

    @Test
    void failedPartitionFailsWholeJob() throws Exception {
        TestWorkerNode coordinator = coordinator(10);
        TestWorkerNode failingWorker = worker(20);
        failingWorker.failWith(new IllegalStateException("partition failed"));
        coordinator.setDiscoveredWorkers(List.of(coordinator, stub(failingWorker)));

        assertThrows(RemoteException.class, () -> coordinator.submitJob(
                JobRequest.max("failed-job", List.of(1, 2, 3, 4))));
    }

    @Test
    void shutdownStopsCoordinatorDispatchExecutor() throws Exception {
        TestWorkerNode coordinator = coordinator(10);
        coordinator.setDiscoveredWorkers(List.of(coordinator));
        coordinator.submitJob(JobRequest.max("first-job", List.of(1)));

        coordinator.shutdownWorkerExecutors();

        assertTrue(coordinator.isDispatchExecutorShutdown());
        assertTrue(coordinator.isComputeExecutorShutdown());
        assertThrows(RemoteException.class, () -> coordinator.submitJob(
                JobRequest.max("later-job", List.of(2))));
    }

    private TestWorkerNode coordinator(int workerId) throws Exception {
        TestWorkerNode coordinator = worker(workerId);
        coordinator.propagateCoordinator(
                "coordinator-" + workerId, workerId, coordinator, null);
        return coordinator;
    }

    private TestWorkerNode worker(int workerId) throws RemoteException {
        TestWorkerNode worker = new TestWorkerNode(workerId);
        workers.add(worker);
        return worker;
    }

    private static WorkerInterface stub(WorkerNode worker) throws Exception {
        return (WorkerInterface) UnicastRemoteObject.toStub(worker);
    }

    private static void assertRange(WorkUnit unit, int start, int end) {
        assertEquals(start, unit.start());
        assertEquals(end, unit.end());
    }

    private static final class TestWorkerNode extends WorkerNode {
        private volatile List<WorkerInterface> discoveredWorkers = List.of();
        private volatile CountDownLatch started;
        private volatile CountDownLatch release;
        private volatile RuntimeException failure;
        private WorkUnit lastWorkUnit;
        private int executionCount;

        private TestWorkerNode(int workerId) throws RemoteException {
            super(workerId, true);
        }

        private void setDiscoveredWorkers(List<WorkerInterface> discoveredWorkers) {
            this.discoveredWorkers = List.copyOf(discoveredWorkers);
        }

        private void blockWith(CountDownLatch started, CountDownLatch release) {
            this.started = started;
            this.release = release;
        }

        private void failWith(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        protected List<WorkerInterface> discoverActiveWorkers() {
            return discoveredWorkers;
        }

        @Override
        protected WorkResult computeWorkUnit(WorkUnit workUnit) {
            executionCount++;
            lastWorkUnit = workUnit;

            CountDownLatch startedLatch = started;
            CountDownLatch releaseLatch = release;
            if (startedLatch != null && releaseLatch != null) {
                startedLatch.countDown();
                try {
                    releaseLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("test dispatch interrupted", e);
                }
            }

            if (failure != null) {
                throw failure;
            }
            return super.computeWorkUnit(workUnit);
        }
    }
}
