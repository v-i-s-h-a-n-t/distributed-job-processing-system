package com.cs324a1.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cs324a1.common.JobRequest;
import com.cs324a1.common.WorkResult;
import com.cs324a1.common.WorkUnit;
import com.cs324a1.common.WorkerInterface;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CoordinatorTermConcurrencyTest {

    private final List<TermTestWorker> workers = new ArrayList<>();

    @AfterEach
    void cleanUpWorkers() {
        for (TermTestWorker worker : workers) {
            worker.shutdownWorkerExecutors();
            try {
                UnicastRemoteObject.unexportObject(worker, true);
            } catch (Exception ignored) {
                // already unexported
            }
        }
    }

    @Test
    void concurrentTermAdmitsOnlyFiveJobsAndTriggersOneReelection() throws Exception {
        TermTestWorker coordinator = coordinator(40);
        CountDownLatch admittedJobsReachedDiscovery = new CountDownLatch(5);
        CountDownLatch releaseDiscovery = new CountDownLatch(1);
        coordinator.blockDiscovery(admittedJobsReachedDiscovery, releaseDiscovery);
        ExecutorService callers = Executors.newFixedThreadPool(8);
        CountDownLatch callersReady = new CountDownLatch(8);
        CountDownLatch startTogether = new CountDownLatch(1);
        List<Future<Long>> submissions = new ArrayList<>();

        try {
            for (int index = 0; index < 8; index++) {
                int jobNumber = index;
                submissions.add(callers.submit(() -> {
                    callersReady.countDown();
                    startTogether.await();
                    return coordinator.submitJob(JobRequest.max(
                            "term-job-" + jobNumber, List.of(jobNumber)));
                }));
            }

            assertTrue(callersReady.await(2, TimeUnit.SECONDS));
            startTogether.countDown();
            assertTrue(admittedJobsReachedDiscovery.await(2, TimeUnit.SECONDS));

            assertEquals(5, coordinator.getJAC());
            assertEquals(5, coordinator.getJobsInCurrentTerm());
            assertEquals(0, coordinator.reelectionCount.get());
            assertFalse(coordinator.reelectionStarted.await(100, TimeUnit.MILLISECONDS));

            releaseDiscovery.countDown();

            int completed = 0;
            int rejected = 0;
            for (Future<Long> submission : submissions) {
                try {
                    submission.get(3, TimeUnit.SECONDS);
                    completed++;
                } catch (ExecutionException e) {
                    assertTrue(e.getCause() instanceof RemoteException);
                    rejected++;
                }
            }

            assertEquals(5, completed);
            assertEquals(3, rejected);
            assertEquals(5, coordinator.getJAC());
            assertTrue(coordinator.reelectionStarted.await(2, TimeUnit.SECONDS));
            assertEquals(1, coordinator.reelectionCount.get());
        } finally {
            startTogether.countDown();
            releaseDiscovery.countDown();
            callers.shutdownNow();
        }
    }

    @Test
    void failedFifthJobStillReleasesInFlightAccounting() throws Exception {
        TermTestWorker coordinator = coordinator(50);
        for (int index = 0; index < 4; index++) {
            coordinator.recordJobAssignment();
        }
        coordinator.failJob("failing-fifth-job");

        assertThrows(RemoteException.class, () -> coordinator.submitJob(
                JobRequest.max("failing-fifth-job", List.of(1))));

        assertEquals(5, coordinator.getJAC());
        assertTrue(coordinator.reelectionStarted.await(2, TimeUnit.SECONDS));
        assertEquals(1, coordinator.reelectionCount.get());
    }

    @Test
    void termLockIsNotHeldWhileAdmittedJobsCompute() throws Exception {
        TermTestWorker coordinator = coordinator(60);
        CountDownLatch bothComputationsStarted = new CountDownLatch(2);
        CountDownLatch releaseComputations = new CountDownLatch(1);
        coordinator.blockComputation(bothComputationsStarted, releaseComputations);
        ExecutorService callers = Executors.newFixedThreadPool(2);

        try {
            Future<Long> first = callers.submit(() -> coordinator.submitJob(
                    JobRequest.max("first-job", List.of(4))));
            Future<Long> second = callers.submit(() -> coordinator.submitJob(
                    JobRequest.max("second-job", List.of(9))));

            assertTrue(bothComputationsStarted.await(2, TimeUnit.SECONDS),
                    "both admitted jobs should compute concurrently outside the term lock");
            assertEquals(2, coordinator.getJAC());
            assertEquals(2, coordinator.getJobsInCurrentTerm());

            releaseComputations.countDown();
            assertEquals(4L, first.get(2, TimeUnit.SECONDS));
            assertEquals(9L, second.get(2, TimeUnit.SECONDS));
            assertEquals(0, coordinator.reelectionCount.get());
        } finally {
            releaseComputations.countDown();
            callers.shutdownNow();
        }
    }

    private TermTestWorker coordinator(int workerId) throws Exception {
        TermTestWorker worker = new TermTestWorker(workerId);
        workers.add(worker);
        worker.propagateCoordinator(
                "term-test-coordinator-" + workerId, workerId, worker, null);
        return worker;
    }

    private static final class TermTestWorker extends WorkerNode {
        private final AtomicInteger reelectionCount = new AtomicInteger();
        private final CountDownLatch reelectionStarted = new CountDownLatch(1);
        private volatile CountDownLatch discoveryStarted;
        private volatile CountDownLatch discoveryRelease;
        private volatile CountDownLatch computationStarted;
        private volatile CountDownLatch computationRelease;
        private volatile String failingJobId;

        private TermTestWorker(int workerId) throws RemoteException {
            super(workerId, true);
        }

        private void blockDiscovery(CountDownLatch started, CountDownLatch release) {
            discoveryStarted = started;
            discoveryRelease = release;
        }

        private void blockComputation(CountDownLatch started, CountDownLatch release) {
            computationStarted = started;
            computationRelease = release;
        }

        private void failJob(String jobId) {
            failingJobId = jobId;
        }

        @Override
        protected List<WorkerInterface> discoverActiveWorkers() throws RemoteException {
            CountDownLatch started = discoveryStarted;
            CountDownLatch release = discoveryRelease;
            if (started != null && release != null) {
                started.countDown();
                awaitLatch(release, "discovery");
            }
            return List.of(this);
        }

        @Override
        protected WorkResult computeWorkUnit(WorkUnit workUnit) {
            CountDownLatch started = computationStarted;
            CountDownLatch release = computationRelease;
            if (started != null && release != null) {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("test computation interrupted", e);
                }
            }
            if (workUnit.jobId().equals(failingJobId)) {
                throw new IllegalStateException("test job failure");
            }
            return super.computeWorkUnit(workUnit);
        }

        @Override
        public void initiateElection() throws RemoteException {
            reelectionCount.incrementAndGet();
            reelectionStarted.countDown();
            super.initiateElection();
        }

        private static void awaitLatch(CountDownLatch latch, String operation)
                throws RemoteException {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RemoteException("test " + operation + " interrupted", e);
            }
        }
    }
}
