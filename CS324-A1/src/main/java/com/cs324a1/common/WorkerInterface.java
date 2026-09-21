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

    // --- Legacy signatures (Member 1) kept for backward compatibility ---
    void receiveElectionMessage(String messageId, int initiatorJAC, int initiatorId) throws RemoteException;
    void receiveCoordinatorMessage(String messsageId, int coordinatorId, WorkerInterface coordinatorRef) throws RemoteException;

    // --- Member 2: Leader Election Extensions ---
    int getJAC() throws RemoteException;
    int getCoordinatorId() throws RemoteException;
    WorkerInterface getCoordinator() throws RemoteException;
    boolean isCoordinator() throws RemoteException;

    /**
     * Convergecast election propagation.
     * @param electionId unique election identifier (e.g. ELECTION-<initiator>-<seq>)
     * @param sender the worker that forwarded this message (null for initiator)
     * @return best candidate in the sender's subtree (null if duplicate)
     */
    Candidate propagateElection(String electionId, WorkerInterface sender) throws RemoteException;

    /**
     * Flood COORDINATOR announcement.
     * @param coordinatorMessageId unique coordinator message id (e.g. COORDINATOR-<electionId>)
     * @param coordinatorId elected coordinator's workerId
     * @param coordinatorRef RMI stub of elected coordinator
     * @param sender forwarder to avoid echo (null for originator)
     */
    void propagateCoordinator(String coordinatorMessageId, int coordinatorId, WorkerInterface coordinatorRef, WorkerInterface sender) throws RemoteException;

    void initiateElection() throws RemoteException;

    // JAC / Term management (Member 2 - term limit 5 jobs)
    void incrementJAC() throws RemoteException;
    int recordJobAssignment() throws RemoteException; // increments JAC + term count, triggers re-election if term >=5
    int getJobsInCurrentTerm() throws RemoteException;
}
