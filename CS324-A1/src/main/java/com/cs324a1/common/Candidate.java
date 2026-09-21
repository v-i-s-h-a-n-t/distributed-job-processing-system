package com.cs324a1.common;

import java.io.Serializable;

/**
 * Election candidate representing a worker's candidacy.
 * Comparable logic: lowest JAC wins; tie -> highest ID wins.
 * Member 2 deliverable – JAC tracking.
 *
 * @author Member 2
 */
public class Candidate implements Serializable {
    private static final long serialVersionUID = 1L;

    public final int workerId;
    public final int jac;
    /** RMI stub of the candidate worker – carried so initiator need not lookup */
    public final WorkerInterface stub;

    public Candidate(int workerId, int jac) {
        this(workerId, jac, null);
    }

    public Candidate(int workerId, int jac, WorkerInterface stub) {
        this.workerId = workerId;
        this.jac = jac;
        this.stub = stub;
    }

    /**
     * Returns true if this candidate is better than other per election rule.
     * Lower JAC wins; if equal JAC higher ID wins.
     */
    public boolean isBetterThan(Candidate other) {
        if (other == null) return true;
        if (this.jac < other.jac) return true;
        if (this.jac > other.jac) return false;
        return this.workerId > other.workerId;
    }

    public static Candidate betterOf(Candidate a, Candidate b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.isBetterThan(b) ? a : b;
    }

    @Override
    public String toString() {
        return "Candidate{id=" + workerId + ", jac=" + jac + "}";
    }
}
