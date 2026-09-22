/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.cs324a1.bootstrap;

import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.cs324a1.common.BootstrapInterface;
import com.cs324a1.common.WorkerInterface;

/**
 *
 * @author Vishant - S11230430
 */
public class BootstrapNode extends UnicastRemoteObject implements BootstrapInterface {

    private static final int HEALTH_CHECK_INTERVAL_SECONDS = 15;

    private final ConcurrentHashMap<Integer, String> activeWorkers;
    private final ScheduledExecutorService healthChecker;

    public BootstrapNode() throws RemoteException {
        super();
        this.activeWorkers = new ConcurrentHashMap<>();
        this.healthChecker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bootstrap-health-check");
            t.setDaemon(true);
            return t;
        });
        this.healthChecker.scheduleWithFixedDelay(
                this::pruneDeadWorkers,
                HEALTH_CHECK_INTERVAL_SECONDS,
                HEALTH_CHECK_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
    }

    @Override
    public synchronized String registerWorker(int workerId, String rmiAddress) throws RemoteException {
        if (activeWorkers.containsKey(workerId)) {
            throw new RemoteException("Worker ID " + workerId + " is already registered at "
                    + activeWorkers.get(workerId) + ". Choose a different worker ID.");
        }

        String randomNeighborAddress = null;

        // If there are already workers in the network, pick a random one for the new node to connect to
        if (!activeWorkers.isEmpty()) {
            List<String> currentNodes = new ArrayList<>(activeWorkers.values());
            int randomIndex = ThreadLocalRandom.current().nextInt(currentNodes.size());
            randomNeighborAddress = currentNodes.get(randomIndex);
        }

        // Add the new worker to the directory
        activeWorkers.put(workerId, rmiAddress);
        System.out.printf("Bootstrap Worker %d registered successfully at %s%n", workerId, rmiAddress);

        return randomNeighborAddress;
    }

    @Override
    public synchronized void deregisterWorker(int workerId) throws RemoteException {
        if (activeWorkers.remove(workerId) != null) {
            System.out.printf("Bootstrap Worker %d deregistered.%n", workerId);
        }
    }

    @Override
    public List<String> getActiveWorkers() throws RemoteException {
        return new ArrayList<>(activeWorkers.values());
    }

    /**
     * Periodically pings every registered worker and removes any that no
     * longer respond, so dead addresses stop being handed out as
     * neighbor candidates to newly joining workers.
     */
    private void pruneDeadWorkers() {
        for (Map.Entry<Integer, String> entry : activeWorkers.entrySet()) {
            int workerId = entry.getKey();
            String rmiAddress = entry.getValue();
            try {
                WorkerInterface stub = (WorkerInterface) Naming.lookup(rmiAddress);
                stub.getWorkerId();
            } catch (RemoteException | NotBoundException | java.net.MalformedURLException e) {
                if (activeWorkers.remove(workerId, rmiAddress)) {
                    System.out.printf("Bootstrap Worker %d at %s failed health check, removing.%n",
                            workerId, rmiAddress);
                }
            }
        }
    }

    private void shutdown(Registry registry) {
        try {
            registry.unbind("BootstrapNode");
        } catch (RemoteException | NotBoundException e) {
            // best-effort on shutdown
        }
        healthChecker.shutdownNow();
    }

    public static void main(String[] args) {
        try {
            int port = 1099;
            if (args.length > 0) {
                try {
                    port = Integer.parseInt(args[0]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid port argument '" + args[0] + "', using default " + port);
                }
            }

            Registry registry;

            // Attempt to create the RMI registry programmatically
            try {
                registry = LocateRegistry.createRegistry(port);
                System.out.println("Bootstrap RMI Registry created dynamically on port " + port);
            } catch (RemoteException e) {
                // If it already exists, locate it instead
                registry = LocateRegistry.getRegistry(port);
                System.out.println("Bootstrap Existing RMI Registry located on port " + port);
            }

            // Instantiate and bind the Bootstrap Node to the registry
            BootstrapNode bootstrap = new BootstrapNode();
            registry.rebind("BootstrapNode", bootstrap);
            System.out.println("Bootstrap Node service bound and running. Ready for workers...");

            final Registry finalRegistry = registry;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Bootstrap Node shutting down, unbinding...");
                bootstrap.shutdown(finalRegistry);
            }));
        } catch (Exception e) {
            System.err.println("Bootstrap Critical failure during startup:");
            e.printStackTrace();
        }
    }
}