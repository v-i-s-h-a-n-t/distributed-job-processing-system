/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.cs324a1.common;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 *
 * @author Vishant - S11230430
 */
public interface WorkerInterface extends Remote {
    int getWorkerId() throws RemoteException;
    String getRmiAddress() throws RemoteException;
    void addNeighbor(WorkerInterface neighbor) throws RemoteException;
    void receiveElectionMessage(String messageId, int initiatorJAC, int initiatorId) throws RemoteException;
    void receiveCoordinatorMessage(String messsageId, int coordinatorId, WorkerInterface coordinatorRef) throws RemoteException;
    
}
