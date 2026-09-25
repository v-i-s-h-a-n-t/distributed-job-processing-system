package com.cs324a1.client;

import com.cs324a1.common.BootstrapInterface;
import com.cs324a1.common.JobRequest;
import com.cs324a1.common.WorkerInterface;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

/**
 * Thin RMI facade used by the Swing client.
 *
 * <p>Discovery is intentionally simple: ask the Bootstrap for active workers
 * and submit to any reachable worker. {@code WorkerNode.submitJob} forwards
 * to the current coordinator, so the client does not need to track leader
 * changes itself. Failed workers are skipped for failover.
 */
public class ClientService {

    private final String bootstrapHost;
    private final int bootstrapPort;

    public ClientService(String bootstrapHost, int bootstrapPort) {
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
    }

    public long submit(JobRequest request) throws Exception {
        List<String> addresses = activeWorkerAddresses();
        if (addresses.isEmpty()) {
            throw new RemoteException("no active workers registered at bootstrap");
        }
        // Prefer a live coordinator if one can be identified, otherwise try in order.
        List<WorkerInterface> stubs = resolveAll(addresses);
        if (stubs.isEmpty()) {
            throw new RemoteException("could not resolve any worker stub");
        }
        stubs.sort((a, b) -> Boolean.compare(!isCoordinatorQuietly(b), !isCoordinatorQuietly(a)));

        RemoteException lastFailure = null;
        for (WorkerInterface worker : stubs) {
            try {
                return worker.submitJob(request);
            } catch (RemoteException e) {
                lastFailure = e;
            }
        }
        throw new RemoteException("all workers failed", lastFailure);
    }

    public List<String> activeWorkerAddresses() throws Exception {
        Registry registry = LocateRegistry.getRegistry(bootstrapHost, bootstrapPort);
        BootstrapInterface bootstrap = (BootstrapInterface) registry.lookup("BootstrapNode");
        return bootstrap.getActiveWorkers();
    }

    private List<WorkerInterface> resolveAll(List<String> addresses) {
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry(bootstrapHost, bootstrapPort);
        } catch (Exception ignored) {
        }
        java.util.ArrayList<WorkerInterface> stubs = new ArrayList<>();
        for (String address : addresses) {
            try {
                if (address != null && address.startsWith("//")) {
                    stubs.add((WorkerInterface) Naming.lookup(address));
                } else if (registry != null) {
                    String name = address;
                    if (name != null && name.contains("/")) {
                        name = name.substring(name.lastIndexOf('/') + 1);
                    }
                    stubs.add((WorkerInterface) registry.lookup(name));
                }
            } catch (Exception ignored) {
                // Skip unreachable worker; next one will be tried.
            }
        }
        return stubs;
    }

    private static boolean isCoordinatorQuietly(WorkerInterface worker) {
        try {
            return worker.isCoordinator();
        } catch (Exception e) {
            return false;
        }
    }
}
