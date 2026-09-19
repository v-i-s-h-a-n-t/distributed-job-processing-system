/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.cs324a1.bootstrap;

import com.cs324a1.common.BootstrapInterface;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author Vishant - S11230430
 */
public class BootstrapNode extends UnicastRemoteObject implements BootstrapInterface {
    private final ConcurrentHashMap<Integer, String> activeWorkers;
    
    public BootstrapNode() throws RemoteException {
        super();
        this.activeWorkers = new ConcurrentHashMap<>();
    }
    
    @Override
    public synchronized String registerWorker(int workerId, String rmiAddress) throws RemoteException {
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
    public static void main(String[] args) {
        try {
            int port = 1099;
            Registry registry;
            
            // Attempt to create the RMI registry programmatically
            try{
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
        } catch (Exception e) {
            System.err.println("[Bootstrap] Critical failure during startup:");
            e.printStackTrace();
        }
    } 
}
