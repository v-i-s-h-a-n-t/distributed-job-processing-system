/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.cs324a1.common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 *
 * @author Vishant - S11230430
 */
public interface BootstrapInterface extends Remote {
    String registerWorker(int workerId, String rmiAddress) throws RemoteException;
    void deregisterWorker(int workerId) throws RemoteException;
    List<String> getActiveWorkers() throws RemoteException;
}
