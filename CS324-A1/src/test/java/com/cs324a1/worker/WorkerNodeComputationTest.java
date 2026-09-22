package com.cs324a1.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cs324a1.common.ComputeOperation;
import com.cs324a1.common.WorkResult;
import com.cs324a1.common.WorkUnit;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class WorkerNodeComputationTest {

    private final List<WorkerNode> workers = new ArrayList<>();

    @AfterEach
    void cleanUpWorkers() {
        for (WorkerNode worker : workers) {
            worker.shutdownComputeExecutor();
            try {
                UnicastRemoteObject.unexportObject(worker, true);
            } catch (Exception ignored) {
                // already unexported
            }
        }
    }

    @Test
    void executesMaxAndPreservesResultIdentity() throws Exception {
        WorkerNode worker = track(new WorkerNode(1, true));
        WorkUnit unit = WorkUnit.max("job-max", 3, List.of(-9, -2, -5));

        WorkResult result = worker.executeWorkUnit(unit);

        assertEquals("job-max", result.jobId());
        assertEquals(3, result.partitionId());
        assertEquals(ComputeOperation.MAX, result.operation());
        assertEquals(-2L, result.value());
    }

    @Test
    void executesPrimeSum() throws Exception {
        WorkerNode worker = track(new WorkerNode(2, true));

        WorkResult result = worker.executeWorkUnit(
                WorkUnit.primeSum("job-sum", 0, 1, 10));

        assertEquals(17L, result.value());
    }

    @Test
    void executesPrimeCount() throws Exception {
        WorkerNode worker = track(new WorkerNode(3, true));

        WorkResult result = worker.executeWorkUnit(
                WorkUnit.primeCount("job-count", 1, List.of(2, 2, 4, 5, 8)));

        assertEquals(3L, result.value());
    }

    @Test
    void executesMultipleWorkUnitsConcurrently() throws Exception {
        BlockingWorkerNode worker = track(new BlockingWorkerNode(4));
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            Future<WorkResult> first = callers.submit(() -> worker.executeWorkUnit(
                    WorkUnit.max("job-a", 0, List.of(1, 8))));
            Future<WorkResult> second = callers.submit(() -> worker.executeWorkUnit(
                    WorkUnit.max("job-b", 0, List.of(3, 9))));

            assertTrue(worker.bothTasksStarted.await(2, TimeUnit.SECONDS),
                    "both tasks should enter the compute pool before either is released");
            worker.releaseTasks.countDown();

            assertEquals(8L, first.get(2, TimeUnit.SECONDS).value());
            assertEquals(9L, second.get(2, TimeUnit.SECONDS).value());
        } finally {
            worker.releaseTasks.countDown();
            callers.shutdownNow();
        }
    }

    @Test
    void propagatesComputationFailureAsRemoteException() throws Exception {
        IllegalStateException failure = new IllegalStateException("test failure");
        FailingWorkerNode worker = track(new FailingWorkerNode(5, failure));

        RemoteException exception = assertThrows(RemoteException.class,
                () -> worker.executeWorkUnit(WorkUnit.max("job", 0, List.of(1))));

        assertSame(failure, exception.getCause());
    }

    @Test
    void shutdownStopsAcceptingComputeTasks() throws Exception {
        WorkerNode worker = track(new WorkerNode(6, true));
        worker.executeWorkUnit(WorkUnit.max("job", 0, List.of(1)));

        worker.shutdownComputeExecutor();

        assertTrue(worker.isComputeExecutorShutdown());
        assertThrows(RemoteException.class,
                () -> worker.executeWorkUnit(WorkUnit.max("later-job", 0, List.of(2))));
    }

    @Test
    void existingElectionBehaviorStillWorksInTestMode() throws Exception {
        WorkerNode first = track(new WorkerNode(7, true));
        WorkerNode second = track(new WorkerNode(8, true));
        first.setJAC(3);
        second.setJAC(1);
        first.addNeighbor(second);
        second.addNeighbor(first);

        first.initiateElection();

        assertEquals(8, first.getCoordinatorId());
        assertEquals(8, second.getCoordinatorId());
        assertTrue(second.isCoordinator());
    }

    private <T extends WorkerNode> T track(T worker) {
        workers.add(worker);
        return worker;
    }

    private static final class BlockingWorkerNode extends WorkerNode {
        private final CountDownLatch bothTasksStarted = new CountDownLatch(2);
        private final CountDownLatch releaseTasks = new CountDownLatch(1);

        private BlockingWorkerNode(int workerId) throws RemoteException {
            super(workerId, true);
        }

        @Override
        protected WorkResult computeWorkUnit(WorkUnit workUnit) {
            bothTasksStarted.countDown();
            try {
                releaseTasks.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("test computation interrupted", e);
            }
            return super.computeWorkUnit(workUnit);
        }
    }

    private static final class FailingWorkerNode extends WorkerNode {
        private final RuntimeException failure;

        private FailingWorkerNode(int workerId, RuntimeException failure) throws RemoteException {
            super(workerId, true);
            this.failure = failure;
        }

        @Override
        protected WorkResult computeWorkUnit(WorkUnit workUnit) {
            throw failure;
        }
    }
}
