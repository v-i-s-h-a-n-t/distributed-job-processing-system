package com.cs324a1.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cs324a1.bootstrap.BootstrapNode;
import com.cs324a1.common.JobRequest;
import com.cs324a1.common.WorkResult;
import com.cs324a1.common.WorkUnit;
import com.cs324a1.common.WorkerInterface;
import java.net.ServerSocket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class DistributedRmiIntegrationTest {

    @Test
    void completeWorkflowUsesBootstrapAndRemoteWorkersForAllOperations() throws Exception {
        try (RmiCluster cluster = new RmiCluster(10, 20, 30)) {
            cluster.announceCoordinator(10);
            WorkerInterface coordinator = cluster.stub(10);

            assertEquals(3, cluster.bootstrap.getActiveWorkers().size());
            assertTrue(cluster.worker(10).getNeighborsList().size() >= 1);
            assertTrue(cluster.worker(20).getNeighborsList().size() >= 1);
            assertTrue(cluster.worker(30).getNeighborsList().size() >= 1);

            long maximum = coordinator.submitJob(JobRequest.max(
                    "rmi-max", List.of(-9, -4, -1, -7, -20, -3, -8, -6, -2, -5)));

            assertEquals(-1L, maximum);
            assertPartition(cluster.worker(10), "rmi-max", 0, List.of(-9, -4, -1, -7));
            assertPartition(cluster.worker(20), "rmi-max", 1, List.of(-20, -3, -8));
            assertPartition(cluster.worker(30), "rmi-max", 2, List.of(-6, -2, -5));

            assertEquals(77L, coordinator.submitJob(
                    JobRequest.primeSum("rmi-sum", 1, 20)));
            assertRangePartition(cluster.worker(10), "rmi-sum", 0, 1, 7);
            assertRangePartition(cluster.worker(20), "rmi-sum", 1, 8, 14);
            assertRangePartition(cluster.worker(30), "rmi-sum", 2, 15, 20);

            assertEquals(4L, coordinator.submitJob(JobRequest.primeCount(
                    "rmi-count", List.of(2, 2, 4, 5, 1, 11, 12))));
            assertPartition(cluster.worker(10), "rmi-count", 0, List.of(2, 2, 4));
            assertPartition(cluster.worker(20), "rmi-count", 1, List.of(5, 1));
            assertPartition(cluster.worker(30), "rmi-count", 2, List.of(11, 12));
        }
    }

    @Test
    void nonCoordinatorSubmissionForwardsOverRmi() throws Exception {
        try (RmiCluster cluster = new RmiCluster(10, 20)) {
            cluster.announceCoordinator(10);

            long result = cluster.stub(20).submitJob(
                    JobRequest.max("rmi-forward", List.of(3, 18, 7, 12)));

            assertEquals(18L, result);
            assertEquals(1, cluster.stub(10).getJAC());
            assertFalse(cluster.stub(20).isCoordinator());
            assertEquals(2, cluster.executionsFor("rmi-forward"));
        }
    }

    @Test
    void remoteDispatchWorkersAndIndependentJobsRunConcurrently() throws Exception {
        try (RmiCluster cluster = new RmiCluster(10, 20)) {
            cluster.announceCoordinator(10);
            CountDownLatch allPartitionsStarted = new CountDownLatch(4);
            CountDownLatch releasePartitions = new CountDownLatch(1);
            cluster.worker(10).blockComputations(allPartitionsStarted, releasePartitions);
            cluster.worker(20).blockComputations(allPartitionsStarted, releasePartitions);
            ExecutorService clients = Executors.newFixedThreadPool(2);

            try {
                Future<Long> first = clients.submit(() -> cluster.stub(10).submitJob(
                        JobRequest.max("rmi-concurrent-max", List.of(1, 9, 3, 7))));
                Future<Long> second = clients.submit(() -> cluster.stub(10).submitJob(
                        JobRequest.primeCount(
                                "rmi-concurrent-count", List.of(2, 4, 5, 11))));

                assertTrue(allPartitionsStarted.await(3, TimeUnit.SECONDS),
                        "both jobs should dispatch all partitions before any is released");
                assertEquals(2, cluster.stub(10).getJAC());

                releasePartitions.countDown();
                assertEquals(9L, first.get(3, TimeUnit.SECONDS));
                assertEquals(3L, second.get(3, TimeUnit.SECONDS));
            } finally {
                releasePartitions.countDown();
                clients.shutdownNow();
            }
        }
    }

    @Test
    void remotePartitionFailureFailsTheCompleteJob() throws Exception {
        try (RmiCluster cluster = new RmiCluster(10, 20)) {
            cluster.announceCoordinator(10);
            cluster.worker(20).failJob("rmi-failure");

            assertThrows(RemoteException.class, () -> cluster.stub(10).submitJob(
                    JobRequest.max("rmi-failure", List.of(4, 9))));
            assertEquals(1, cluster.stub(10).getJAC());
        }
    }

    @Test
    void bootstrapNeighboursAndElectionRulesWorkAcrossRmi() throws Exception {
        try (RmiCluster cluster = new RmiCluster(10, 20, 30)) {
            cluster.worker(10).setJAC(4);
            cluster.worker(20).setJAC(1);
            cluster.worker(30).setJAC(1);

            cluster.stub(10).initiateElection();

            assertEquals(30, cluster.stub(10).getCoordinatorId());
            assertEquals(30, cluster.stub(20).getCoordinatorId());
            assertEquals(30, cluster.stub(30).getCoordinatorId());
            assertTrue(cluster.stub(30).isCoordinator());
            assertFalse(cluster.stub(20).isCoordinator());
        }
    }

    private static void assertPartition(
            RecordingWorker worker,
            String jobId,
            int partitionId,
            List<Integer> expectedNumbers) {
        WorkUnit unit = worker.execution(jobId);
        assertEquals(partitionId, unit.partitionId());
        assertEquals(expectedNumbers, unit.numbers());
    }

    private static void assertRangePartition(
            RecordingWorker worker,
            String jobId,
            int partitionId,
            int expectedStart,
            int expectedEnd) {
        WorkUnit unit = worker.execution(jobId);
        assertEquals(partitionId, unit.partitionId());
        assertEquals(expectedStart, unit.start());
        assertEquals(expectedEnd, unit.end());
    }

    private static final class RmiCluster implements AutoCloseable {
        private final int port;
        private final Registry registry;
        private final BootstrapNode bootstrap;
        private final List<RecordingWorker> workers = new ArrayList<>();

        private RmiCluster(int... workerIds) throws Exception {
            port = availablePort();
            registry = LocateRegistry.createRegistry(port);
            bootstrap = new BootstrapNode();
            registry.rebind("BootstrapNode", bootstrap);
            for (int workerId : workerIds) {
                workers.add(new RecordingWorker(workerId, port));
            }
        }

        private RecordingWorker worker(int workerId) {
            return workers.stream()
                    .filter(worker -> worker.localWorkerId() == workerId)
                    .findFirst()
                    .orElseThrow();
        }

        private WorkerInterface stub(int workerId) throws Exception {
            return (WorkerInterface) registry.lookup("Worker-" + workerId);
        }

        private void announceCoordinator(int workerId) throws Exception {
            WorkerInterface coordinator = stub(workerId);
            coordinator.propagateCoordinator(
                    "integration-coordinator-" + port, workerId, coordinator, null);
        }

        private int executionsFor(String jobId) {
            return workers.stream()
                    .mapToInt(worker -> worker.executionCount(jobId))
                    .sum();
        }

        @Override
        public void close() {
            for (RecordingWorker worker : workers) {
                try {
                    bootstrap.deregisterWorker(worker.localWorkerId());
                } catch (RemoteException ignored) {
                    // best-effort test cleanup
                }
                worker.shutdownWorkerExecutors();
                try {
                    registry.unbind("Worker-" + worker.localWorkerId());
                } catch (Exception ignored) {
                    // best-effort test cleanup
                }
                try {
                    UnicastRemoteObject.unexportObject(worker, true);
                } catch (Exception ignored) {
                    // already unexported
                }
            }
            try {
                registry.unbind("BootstrapNode");
            } catch (Exception ignored) {
                // best-effort test cleanup
            }
            try {
                UnicastRemoteObject.unexportObject(bootstrap, true);
            } catch (Exception ignored) {
                // already unexported
            }
            try {
                UnicastRemoteObject.unexportObject(registry, true);
            } catch (Exception ignored) {
                // already unexported
            }
        }

        private static int availablePort() throws Exception {
            try (ServerSocket socket = new ServerSocket(0)) {
                return socket.getLocalPort();
            }
        }
    }

    private static final class RecordingWorker extends WorkerNode {
        private final int localWorkerId;
        private final CopyOnWriteArrayList<WorkUnit> executions =
                new CopyOnWriteArrayList<>();
        private volatile CountDownLatch computationStarted;
        private volatile CountDownLatch computationRelease;
        private volatile String failingJobId;

        private RecordingWorker(int workerId, int port) throws RemoteException {
            super(workerId, "localhost", port);
            localWorkerId = workerId;
        }

        private int localWorkerId() {
            return localWorkerId;
        }

        private void blockComputations(CountDownLatch started, CountDownLatch release) {
            computationStarted = started;
            computationRelease = release;
        }

        private void failJob(String jobId) {
            failingJobId = jobId;
        }

        private WorkUnit execution(String jobId) {
            return executions.stream()
                    .filter(unit -> unit.jobId().equals(jobId))
                    .findFirst()
                    .orElseThrow();
        }

        private int executionCount(String jobId) {
            return (int) executions.stream()
                    .filter(unit -> unit.jobId().equals(jobId))
                    .count();
        }

        @Override
        protected WorkResult computeWorkUnit(WorkUnit workUnit) {
            executions.add(workUnit);
            CountDownLatch started = computationStarted;
            CountDownLatch release = computationRelease;
            if (started != null && release != null) {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("integration computation interrupted", e);
                }
            }
            if (workUnit.jobId().equals(failingJobId)) {
                throw new IllegalStateException("integration partition failure");
            }
            return super.computeWorkUnit(workUnit);
        }
    }
}
