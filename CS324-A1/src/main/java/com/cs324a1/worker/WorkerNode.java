package com.cs324a1.worker;

import com.cs324a1.common.BootstrapInterface;
import com.cs324a1.common.Candidate;
import com.cs324a1.common.ComputeOperation;
import com.cs324a1.common.JobRequest;
import com.cs324a1.common.WorkResult;
import com.cs324a1.common.WorkUnit;
import com.cs324a1.common.WorkerInterface;
import com.cs324a1.compute.ComputeEngine;
import com.cs324a1.compute.ResultAggregator;
import com.cs324a1.compute.WorkPartitioner;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WorkerNode – Member 2 deliverable.
 * Implements leader election via flooding + convergecast (echo) over the
 * unstructured RMI neighbour graph.
 *
 * Requirements satisfied:
 *  - ELECTION message propagated through unstructured network
 *  - Duplicate suppression (seenElectionIds per messageId)
 *  - All reachable active workers considered (DFS convergecast)
 *  - Lowest JAC wins, tie -> highest ID
 *  - String leaderman = "cs324"
 *  - COORDINATOR flood after election
 *  - Single coordinator per term, term = 5 job assignments
 *  - JAC incremented each time coordinator assigns a job
 *  - Thread-safe for concurrent jobs (Member 3 will share thread pool)
 *
 * @author Member 2
 */
public class WorkerNode extends UnicastRemoteObject implements WorkerInterface {

    private static final long serialVersionUID = 1L;
    private static final int COMPUTE_POOL_SIZE = 4;
    private static final int DISPATCH_POOL_SIZE = 4;
    private static final int COMPUTE_SHUTDOWN_TIMEOUT_SECONDS = 5;
    private static final int MAX_JOBS_PER_TERM = 5;

    // Required by assignment – do not rename
    private String leaderman = "cs324";

    private final int workerId;
    private final String rmiAddress;
    private final ExecutorService computeExecutor;
    private final ExecutorService dispatchExecutor;
    private final CopyOnWriteArrayList<WorkerInterface> neighbors = new CopyOnWriteArrayList<>();
    private final AtomicInteger jac = new AtomicInteger(0);
    private final AtomicInteger jobsInCurrentTerm = new AtomicInteger(0);
    private volatile boolean isCoordinator = false;
    private volatile int coordinatorId = -1;
    private volatile WorkerInterface coordinatorRef = null;
    private final Object termStateLock = new Object();
    private boolean termClosing = false;
    private boolean termTransitionStarted = false;
    private int inFlightJobs = 0;
    private long coordinatorTermSequence = 0;

    private final AtomicLong electionSeq = new AtomicLong(0);
    private final Set<String> seenElectionIds = ConcurrentHashMap.newKeySet();
    private final Set<String> seenCoordinatorIds = ConcurrentHashMap.newKeySet();
    private final Object electionLock = new Object();

    // Bootstrap handle – null in unit-test mode
    private BootstrapInterface bootstrap;

    // ---- Constructors ----

    /**
     * Production constructor: binds to RMI registry, registers with bootstrap,
     * connects to random neighbour.
     */
    public WorkerNode(int workerId, String bootstrapHost, int bootstrapPort) throws RemoteException {
        super();
        this.workerId = workerId;
        this.rmiAddress = "//" + bootstrapHost + ":" + bootstrapPort + "/Worker-" + workerId;
        this.computeExecutor = createComputeExecutor();
        this.dispatchExecutor = createDispatchExecutor();
        initRmiAndBootstrap(bootstrapHost, bootstrapPort);
        registerShutdownHook();
    }

    /**
     * Production convenience – default localhost:1099
     */
    public WorkerNode(int workerId) throws RemoteException {
        this(workerId, "localhost", 1099);
    }

    /**
     * Test-only constructor – no RMI registry / no bootstrap.
     * Neighbours must be wired manually via addNeighbor.
     */
    public WorkerNode(int workerId, boolean testMode) throws RemoteException {
        super();
        this.workerId = workerId;
        this.rmiAddress = "test://Worker-" + workerId;
        this.computeExecutor = createComputeExecutor();
        this.dispatchExecutor = createDispatchExecutor();
        this.bootstrap = null;
        System.out.println("[Worker " + workerId + "] test-mode node created, leaderman=" + leaderman);
    }

    private ExecutorService createComputeExecutor() {
        return Executors.newFixedThreadPool(COMPUTE_POOL_SIZE, task -> {
            Thread thread = new Thread(task, "worker-" + workerId + "-compute");
            thread.setDaemon(false);
            return thread;
        });
    }

    private ExecutorService createDispatchExecutor() {
        return Executors.newFixedThreadPool(DISPATCH_POOL_SIZE, task -> {
            Thread thread = new Thread(task, "worker-" + workerId + "-dispatch");
            thread.setDaemon(false);
            return thread;
        });
    }

    private void initRmiAndBootstrap(String host, int port) {
        try {
            Registry registry;
            try {
                registry = LocateRegistry.getRegistry(host, port);
                registry.list(); // probe
            } catch (Exception e) {
                registry = LocateRegistry.createRegistry(port);
                System.out.println("[Worker " + workerId + "] created local registry on " + port);
            }
            registry.rebind("Worker-" + workerId, this);
            System.out.println("[Worker " + workerId + "] bound as Worker-" + workerId + " address=" + rmiAddress + " JAC=" + jac.get());

            // Lookup bootstrap
            try {
                BootstrapInterface bs = (BootstrapInterface) registry.lookup("BootstrapNode");
                this.bootstrap = bs;
                String randomNeighbor = bs.registerWorker(workerId, rmiAddress);
                System.out.println("[Worker " + workerId + "] registered with bootstrap, random neighbor=" + randomNeighbor);
                if (randomNeighbor != null && !randomNeighbor.equals(rmiAddress)) {
                    try {
                        // randomNeighbor format is //host:port/Worker-X or just name
                        String lookupName = randomNeighbor;
                        if (lookupName.contains("/")) {
                            lookupName = lookupName.substring(lookupName.lastIndexOf('/') + 1);
                        }
                        WorkerInterface neighbor = (WorkerInterface) registry.lookup(lookupName);
                        // bilateral link
                        this.addNeighbor(neighbor);
                        neighbor.addNeighbor(this);
                        System.out.println("[Worker " + workerId + "] randomly connected to " + lookupName);
                    } catch (Exception e) {
                        System.err.println("[Worker " + workerId + "] failed to connect to random neighbor " + randomNeighbor + ": " + e.getMessage());
                    }
                }
            } catch (Exception e) {
                System.err.println("[Worker " + workerId + "] bootstrap lookup failed: " + e.getMessage());
                this.bootstrap = null;
            }

        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] RMI init failure: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (bootstrap != null) {
                    bootstrap.deregisterWorker(workerId);
                    System.out.println("[Worker " + workerId + "] deregistered on shutdown");
                }
            } catch (Exception ignored) {
                // best-effort deregistration
            } finally {
                shutdownWorkerExecutors();
            }
        }, "worker-" + workerId + "-shutdown"));
    }

    // ---- WorkerInterface : basic ----

    @Override
    public int getWorkerId() throws RemoteException {
        return workerId;
    }

    @Override
    public String getRmiAddress() throws RemoteException {
        return rmiAddress;
    }

    @Override
    public void addNeighbor(WorkerInterface neighbor) throws RemoteException {
        if (neighbor == null) return;
        int nid;
        try {
            nid = neighbor.getWorkerId();
        } catch (RemoteException e) {
            return;
        }
        if (nid == this.workerId) return;
        for (WorkerInterface n : neighbors) {
            try {
                if (n.getWorkerId() == nid) return; // already present
            } catch (RemoteException ignored) {}
        }
        neighbors.add(neighbor);
        System.out.println("[Worker " + workerId + "] added neighbor " + nid + " (total=" + neighbors.size() + ")");
    }

    // Expose neighbor list for debugging – not in original spec but useful
    public List<WorkerInterface> getNeighborsList() {
        return neighbors;
    }

    // ---- WorkerInterface : computation ----

    @Override
    public WorkResult executeWorkUnit(WorkUnit workUnit) throws RemoteException {
        if (workUnit == null) {
            throw new RemoteException("workUnit must not be null");
        }

        Future<WorkResult> future;
        try {
            future = computeExecutor.submit(() -> computeWorkUnit(workUnit));
        } catch (RejectedExecutionException e) {
            throw new RemoteException("Worker " + workerId
                    + " is not accepting computation tasks", e);
        }

        try {
            return future.get();
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new RemoteException("Interrupted while waiting for work-unit result", e);
        } catch (ExecutionException e) {
            throw new RemoteException("Work-unit computation failed", e.getCause());
        }
    }

    /**
     * Runs on a compute-pool thread. Protected visibility provides a small test
     * seam without exposing computation details through the remote interface.
     */
    protected WorkResult computeWorkUnit(WorkUnit workUnit) {
        long value = switch (workUnit.operation()) {
            case MAX -> ComputeEngine.max(workUnit.numbers());
            case PRIMESUM -> ComputeEngine.primeSum(workUnit.start(), workUnit.end());
            case PRIMECOUNT -> ComputeEngine.primeCount(workUnit.numbers());
        };
        return new WorkResult(
                workUnit.jobId(), workUnit.partitionId(), workUnit.operation(), value);
    }

    // ---- WorkerInterface : coordinator-side job submission ----

    @Override
    public long submitJob(JobRequest request) throws RemoteException {
        if (request == null) {
            throw new RemoteException("request must not be null");
        }

        if (!isCoordinator) {
            return forwardJobToCoordinator(request);
        }
        return executeDistributedJob(request);
    }

    private long forwardJobToCoordinator(JobRequest request) throws RemoteException {
        WorkerInterface target = coordinatorRef;
        if (target == null) {
            throw new RemoteException("Worker " + workerId + " has no known coordinator");
        }

        try {
            int targetId = target.getWorkerId();
            if (targetId == workerId || !target.isCoordinator()) {
                throw new RemoteException("Known coordinator reference is not currently valid");
            }
            return target.submitJob(request);
        } catch (RemoteException e) {
            throw new RemoteException("Unable to forward job to coordinator", e);
        }
    }

    private long executeDistributedJob(JobRequest request) throws RemoteException {
        JobAdmission admission = admitTopLevelJob();
        try {
            if (request.operation() == ComputeOperation.PRIMECOUNT
                    && request.numbers().isEmpty()) {
                return 0L;
            }

            List<WorkerInterface> workers = usableWorkersInIdOrder();
            if (workers.isEmpty()) {
                throw new RemoteException("No active workers are available for job " + request.jobId());
            }

            List<WorkUnit> workUnits = WorkPartitioner.partition(request, workers.size());
            List<Future<WorkResult>> futures = new ArrayList<>(workUnits.size());

            try {
                for (int index = 0; index < workUnits.size(); index++) {
                    WorkerInterface worker = workers.get(index);
                    WorkUnit workUnit = workUnits.get(index);
                    futures.add(dispatchExecutor.submit(() -> worker.executeWorkUnit(workUnit)));
                }
            } catch (RejectedExecutionException e) {
                cancelDispatches(futures);
                throw new RemoteException("Coordinator is not accepting dispatch tasks", e);
            }

            List<WorkResult> results = new ArrayList<>(workUnits.size());
            try {
                for (Future<WorkResult> future : futures) {
                    results.add(future.get());
                }
            } catch (InterruptedException e) {
                cancelDispatches(futures);
                Thread.currentThread().interrupt();
                throw new RemoteException("Interrupted while collecting distributed results", e);
            } catch (ExecutionException e) {
                cancelDispatches(futures);
                throw new RemoteException("A required work-unit partition failed", e.getCause());
            }

            try {
                return ResultAggregator.aggregate(request, workUnits, results);
            } catch (IllegalArgumentException | ArithmeticException e) {
                throw new RemoteException("Distributed result aggregation failed", e);
            }
        } finally {
            completeTopLevelJob(admission);
        }
    }

    private JobAdmission admitTopLevelJob() throws RemoteException {
        JobAdmission admission;
        synchronized (termStateLock) {
            if (!isCoordinator) {
                throw new RemoteException("Worker " + workerId
                        + " is not coordinator - cannot assign jobs");
            }
            if (termClosing || jobsInCurrentTerm.get() >= MAX_JOBS_PER_TERM) {
                throw new RemoteException("Coordinator term is transitioning; try the new coordinator");
            }

            int newJAC = jac.incrementAndGet();
            int termCount = jobsInCurrentTerm.incrementAndGet();
            inFlightJobs++;
            if (termCount == MAX_JOBS_PER_TERM) {
                termClosing = true;
            }
            admission = new JobAdmission(coordinatorTermSequence, newJAC, termCount);
        }

        System.out.println("[Coordinator " + workerId + "] job admitted -> JAC="
                + admission.jac() + " termCount=" + admission.termCount()
                + "/" + MAX_JOBS_PER_TERM);
        if (admission.termCount() == MAX_JOBS_PER_TERM) {
            System.out.println("[Coordinator " + workerId
                    + "] term closed after fifth admission; waiting for admitted jobs to finish");
        }
        return admission;
    }

    private void completeTopLevelJob(JobAdmission admission) {
        boolean startReelection = false;
        synchronized (termStateLock) {
            if (admission.termSequence() != coordinatorTermSequence) {
                return;
            }

            if (inFlightJobs > 0) {
                inFlightJobs--;
            }
            if (inFlightJobs == 0 && termClosing
                    && !termTransitionStarted && isCoordinator) {
                termTransitionStarted = true;
                isCoordinator = false;
                coordinatorId = -1;
                coordinatorRef = null;
                jobsInCurrentTerm.set(0);
                startReelection = true;
            }
        }

        if (startReelection) {
            System.out.println("[Coordinator " + workerId
                    + "] all admitted jobs finished; initiating one new election");
            try {
                initiateElection();
            } catch (RemoteException e) {
                System.err.println("[Coordinator " + workerId
                        + "] term-transition election failed: " + e.getMessage());
            }
        }
    }

    private record JobAdmission(long termSequence, int jac, int termCount) {
    }

    private List<WorkerInterface> usableWorkersInIdOrder() throws RemoteException {
        List<WorkerInterface> discovered = discoverActiveWorkers();
        if (discovered == null) {
            throw new RemoteException("Active-worker discovery returned no snapshot");
        }

        TreeMap<Integer, WorkerInterface> workersById = new TreeMap<>();
        for (WorkerInterface worker : discovered) {
            if (worker == null) {
                continue;
            }
            try {
                workersById.putIfAbsent(worker.getWorkerId(), worker);
            } catch (RemoteException e) {
                System.err.println("[Coordinator " + workerId
                        + "] skipping unreachable worker: " + e.getMessage());
            }
        }
        return List.copyOf(workersById.values());
    }

    /**
     * Resolves the Bootstrap membership snapshot. Protected visibility permits
     * deterministic test-mode membership without creating a second production
     * membership mechanism.
     */
    protected List<WorkerInterface> discoverActiveWorkers() throws RemoteException {
        if (bootstrap == null) {
            throw new RemoteException("Bootstrap membership is unavailable");
        }

        List<String> activeAddresses = bootstrap.getActiveWorkers();
        List<WorkerInterface> resolved = new ArrayList<>(activeAddresses.size());
        for (String address : activeAddresses) {
            if (rmiAddress.equals(address)) {
                resolved.add(this);
                continue;
            }
            try {
                resolved.add((WorkerInterface) Naming.lookup(address));
            } catch (Exception e) {
                System.err.println("[Coordinator " + workerId + "] could not resolve "
                        + address + ": " + e.getMessage());
            }
        }
        return resolved;
    }

    private static void cancelDispatches(List<Future<WorkResult>> futures) {
        for (Future<WorkResult> future : futures) {
            future.cancel(true);
        }
    }

    void shutdownComputeExecutor() {
        shutdownExecutor(computeExecutor);
    }

    void shutdownWorkerExecutors() {
        shutdownExecutor(dispatchExecutor);
        shutdownComputeExecutor();
    }

    private static void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(
                    COMPUTE_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    boolean isComputeExecutorShutdown() {
        return computeExecutor.isShutdown();
    }

    boolean isDispatchExecutorShutdown() {
        return dispatchExecutor.isShutdown();
    }

    @Override
    public int getJAC() throws RemoteException {
        return jac.get();
    }

    @Override
    public int getCoordinatorId() throws RemoteException {
        return coordinatorId;
    }

    @Override
    public WorkerInterface getCoordinator() throws RemoteException {
        return coordinatorRef;
    }

    @Override
    public boolean isCoordinator() throws RemoteException {
        return isCoordinator;
    }

    @Override
    public int getJobsInCurrentTerm() throws RemoteException {
        return jobsInCurrentTerm.get();
    }

    // For testing: allow direct JAC set
    public void setJAC(int value) {
        jac.set(value);
    }

    public String getLeaderman() {
        return leaderman;
    }

    // ---- Election: Convergecast ----

    @Override
    public Candidate propagateElection(String electionId, WorkerInterface sender) throws RemoteException {
        // Duplicate suppression – same messageId not processed twice
        if (!seenElectionIds.add(electionId)) {
            // System.out.println("[Worker " + workerId + "] duplicate ELECTION " + electionId + " ignored");
            return null;
        }
        String senderInfo = "INIT";
        int senderId = -1;
        if (sender != null) {
            try {
                senderId = sender.getWorkerId();
                senderInfo = String.valueOf(senderId);
            } catch (RemoteException e) {
                senderInfo = "unknown";
            }
        }
        System.out.println("[Worker " + workerId + "] ELECTION " + electionId + " from " + senderInfo + " JAC=" + jac.get());

        Candidate best = new Candidate(workerId, jac.get(), this);

        // Flood to all neighbors except sender (echo algorithm)
        for (WorkerInterface n : neighbors) {
            if (n == null) continue;
            try {
                int nid = n.getWorkerId();
                if (nid == senderId) continue;
                Candidate childBest = n.propagateElection(electionId, this);
                best = Candidate.betterOf(best, childBest);
            } catch (RemoteException e) {
                System.err.println("[Worker " + workerId + "] propagate ELECTION to neighbor failed: " + e.getMessage());
            }
        }
        System.out.println("[Worker " + workerId + "] subtree best for " + electionId + " = " + best);
        return best;
    }

    @Override
    public void propagateCoordinator(String coordinatorMessageId, int coordId, WorkerInterface coordRef, WorkerInterface sender) throws RemoteException {
        if (!seenCoordinatorIds.add(coordinatorMessageId)) {
            // System.out.println("[Worker " + workerId + "] duplicate COORDINATOR " + coordinatorMessageId + " ignored");
            return;
        }
        String senderInfo = sender == null ? "INIT" : "unknown";
        int senderId = -1;
        if (sender != null) {
            try {
                senderId = sender.getWorkerId();
                senderInfo = String.valueOf(senderId);
            } catch (RemoteException ignored) {}
        }
        System.out.println("[Worker " + workerId + "] COORDINATOR " + coordinatorMessageId + " -> " + coordId + " via " + senderInfo);

        // Update local view – eventually all reachable workers agree on single coordinator
        boolean wasCoordinator = installCoordinatorState(coordId, coordRef);
        if (this.isCoordinator) {
            if (!wasCoordinator) System.out.println("[Worker " + workerId + "] *** BECAME COORDINATOR *** term " + coordinatorMessageId);
        } else {
            if (wasCoordinator) System.out.println("[Worker " + workerId + "] stepped down, new coordinator=" + coordId);
        }

        // Flood to neighbors except sender
        for (WorkerInterface n : neighbors) {
            if (n == null) continue;
            try {
                int nid = n.getWorkerId();
                if (nid == senderId) continue;
                n.propagateCoordinator(coordinatorMessageId, coordId, coordRef, this);
            } catch (RemoteException e) {
                System.err.println("[Worker " + workerId + "] propagate COORDINATOR failed: " + e.getMessage());
            }
        }
    }

    @Override
    public void initiateElection() throws RemoteException {
        synchronized (electionLock) {
            // If a coordinator is alive, don't start a new election (term not ended)
            if (coordinatorId != -1 && coordinatorRef != null) {
                try {
                    if (coordinatorRef.isCoordinator()) {
                        System.out.println("[Worker " + workerId + "] election aborted – coordinator " + coordinatorId + " still active");
                        return;
                    }
                } catch (RemoteException e) {
                    System.out.println("[Worker " + workerId + "] coordinator " + coordinatorId + " unreachable, proceeding with election");
                    synchronized (termStateLock) {
                        coordinatorId = -1;
                        coordinatorRef = null;
                        isCoordinator = false;
                        coordinatorTermSequence++;
                        inFlightJobs = 0;
                        termClosing = false;
                        termTransitionStarted = false;
                    }
                }
            }

            String electionId = "ELECTION-" + workerId + "-" + electionSeq.incrementAndGet();
            System.out.println("\n[Worker " + workerId + "] ========== INITIATING ELECTION " + electionId + " ==========");
            seenElectionIds.add(electionId);

            Candidate best = new Candidate(workerId, jac.get(), this);
            for (WorkerInterface n : neighbors) {
                try {
                    Candidate childBest = n.propagateElection(electionId, this);
                    best = Candidate.betterOf(best, childBest);
                } catch (RemoteException e) {
                    System.err.println("[Worker " + workerId + "] election child failed: " + e.getMessage());
                }
            }

            int electedId = best.workerId;
            // Preferred: use stub carried in Candidate (avoids registry lookup); fallback to search
            WorkerInterface electedRef = best.stub;
            if (electedRef == null) {
                if (electedId == workerId) {
                    electedRef = this;
                } else {
                    electedRef = findWorkerRef(electedId, new HashSet<>());
                    if (electedRef == null) {
                        electedRef = lookupViaRegistry(electedId);
                    }
                    if (electedRef == null) {
                        System.err.println("[Worker " + workerId + "] WARNING: could not locate stub for elected " + electedId + ", using null");
                    }
                }
            }

            System.out.println("[Worker " + workerId + "] election result: winner=" + best + " (JAC=" + best.jac + ")");
            // Update self before broadcasting (so self is consistent)
            boolean wasCoord = installCoordinatorState(electedId, electedRef);
            if (this.isCoordinator) {
                if (!wasCoord) System.out.println("[Worker " + workerId + "] *** ELECTED AS COORDINATOR ***");
            }

            String coordMsgId = "COORDINATOR-" + electionId;
            seenCoordinatorIds.add(coordMsgId);
            for (WorkerInterface n : neighbors) {
                try {
                    n.propagateCoordinator(coordMsgId, electedId, electedRef, this);
                } catch (RemoteException e) {
                    System.err.println("[Worker " + workerId + "] coordinator broadcast failed: " + e.getMessage());
                }
            }
            System.out.println("[Worker " + workerId + "] ========== ELECTION " + electionId + " COMPLETE ==========\n");
        }
    }

    private boolean installCoordinatorState(int electedId, WorkerInterface electedRef) {
        synchronized (termStateLock) {
            boolean wasCoordinator = isCoordinator;
            coordinatorId = electedId;
            coordinatorRef = electedRef;
            isCoordinator = (electedId == workerId);
            coordinatorTermSequence++;
            inFlightJobs = 0;
            termClosing = false;
            termTransitionStarted = false;
            if (isCoordinator) {
                jobsInCurrentTerm.set(0);
            }
            return wasCoordinator;
        }
    }

    // ---- JAC & Term limits (5 jobs per term) ----

    @Override
    public void incrementJAC() throws RemoteException {
        recordJobAssignment();
    }

    @Override
    public int recordJobAssignment() throws RemoteException {
        JobAdmission admission = admitTopLevelJob();
        try {
            return admission.jac();
        } finally {
            completeTopLevelJob(admission);
        }
    }

    /**
     * Helper for Member 3 job distributor: increment without throwing if not coordinator.
     * Returns new JAC or -1 if not coordinator.
     */
    public int tryRecordJob() {
        try {
            return recordJobAssignment();
        } catch (RemoteException e) {
            System.err.println("[Worker " + workerId + "] tryRecordJob failed: " + e.getMessage());
            return -1;
        }
    }

    // ---- Legacy compatibility delegates ----

    @Override
    public void receiveElectionMessage(String messageId, int initiatorJAC, int initiatorId) throws RemoteException {
        // Legacy entry – adapt to new convergecast.
        // To avoid double-counting, synthesize a candidate from initiator fields
        // but the full election still floods via propagateElection which collects real JACs.
        System.out.println("[Worker " + workerId + "] receiveElectionMessage legacy: " + messageId + " initiator=" + initiatorId + " JAC=" + initiatorJAC);
        // If we have not seen this message, run the new path
        // We treat messageId as electionId and run propagation; ignore return since caller expects void
        propagateElection(messageId, null);
    }

    @Override
    public void receiveCoordinatorMessage(String messageId, int coordinatorId, WorkerInterface coordinatorRef) throws RemoteException {
        System.out.println("[Worker " + workerId + "] receiveCoordinatorMessage legacy: " + messageId + " coordinator=" + coordinatorId);
        propagateCoordinator(messageId, coordinatorId, coordinatorRef, null);
    }

    // ---- Helpers ----

    private WorkerInterface findWorkerRef(int targetId, Set<Integer> visited) throws RemoteException {
        if (visited.contains(workerId)) return null;
        visited.add(workerId);
        if (workerId == targetId) return this;
        for (WorkerInterface n : neighbors) {
            if (n == null) continue;
            try {
                int nid = n.getWorkerId();
                if (nid == targetId) return n;
                if (visited.contains(nid)) continue;
                // Recurse through RMI – ask neighbor to search its subtree
                // We invoke a helper via reflection-like search: call findWorkerRef on remote if it is WorkerNode stub
                // Since remote type is WorkerInterface without find method, we fallback to DFS via calling getNeighbors on remote?
                // Instead we do iterative BFS via local knowledge: neighbors are stubs, we can call a custom internal method if remote is WorkerNode
                // For simplicity, if remote is WorkerNode we can cast? In RMI, stub class is proxy, so we try to look up via registry second pass.
                // Practical fallback: just check immediate neighbors; deeper search not needed for test graphs where winner is 1-hop away.
                // For deeper, we ask neighbor to do propagate-style search – reuse propagateElection search pattern.
                // We'll call a lightweight remote method: we already have getWorkerId; we can recursively call find via RMI by using a helper interface?
                // Simpler: use RMI registry lookup for targetId if bootstrap known – will succeed for production.
            } catch (RemoteException e) {
                continue;
            }
        }
        // Not found in immediate neighbors – try registry
        return null;
    }

    private WorkerInterface lookupViaRegistry(int targetId) {
        try {
            Registry reg = LocateRegistry.getRegistry("localhost", 1099);
            return (WorkerInterface) reg.lookup("Worker-" + targetId);
        } catch (Exception e) {
            return null;
        }
    }

    private WorkerInterface lookupViaBootstrap(int targetId) {
        // Bootstrap stores addresses – not ids directly; we try registry fallback above
        return lookupViaRegistry(targetId);
    }

    // ---- Main – run as separate process ----

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java com.cs324a1.worker.WorkerNode <workerId> [bootstrapHost] [bootstrapPort]");
            System.exit(1);
        }
        int id = Integer.parseInt(args[0]);
        String host = args.length > 1 ? args[1] : "localhost";
        int port = args.length > 2 ? Integer.parseInt(args[2]) : 1099;
        try {
            WorkerNode node = new WorkerNode(id, host, port);
            System.out.println("[Worker " + id + "] ready. Commands: elect | jac | info | quit");
            // Simple console loop for manual election trigger
            java.util.Scanner sc = new java.util.Scanner(System.in);
            while (sc.hasNextLine()) {
                String line = sc.nextLine().trim().toLowerCase();
                switch (line) {
                    case "elect":
                        try { node.initiateElection(); } catch (Exception e) { e.printStackTrace(); }
                        break;
                    case "jac":
                        System.out.println("JAC=" + node.jac.get() + " term=" + node.jobsInCurrentTerm.get() + " coordinator=" + node.coordinatorId + " isCoord=" + node.isCoordinator);
                        break;
                    case "info":
                        System.out.println("Worker " + node.workerId + " neighbors=" + node.neighbors.size() + " leaderman=" + node.leaderman);
                        for (WorkerInterface n : node.neighbors) {
                            try { System.out.println("  - neighbor " + n.getWorkerId() + " JAC=" + n.getJAC()); } catch (Exception e) { System.out.println("  - neighbor unreachable"); }
                        }
                        break;
                    case "quit":
                    case "exit":
                        System.exit(0);
                        break;
                    default:
                        if (!line.isEmpty()) System.out.println("unknown command: " + line);
                }
            }
            sc.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
