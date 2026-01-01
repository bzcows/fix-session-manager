package com.fixgateway.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Represents a deterministic assignment of a FIX session to a specific node.
 * <p>
 * Stored in Hazelcast map {@code fix-session-assignments}.
 * The epoch field provides fencing: each reassignment increments the epoch,
 * ensuring stale nodes cannot act.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SessionAssignment implements Serializable {

    /**
     * Unique key for the FIX session.
     * Format: {@code BeginString:SenderCompID->TargetCompID}
     */
    private String sessionKey;

    /**
     * Hazelcast member UUID of the node assigned to run this FIX session.
     */
    private String assignedNodeId;

    /**
     * Monotonic version number that increments on every reassignment.
     * Used for fencing: a node may only act if its local epoch matches this value.
     */
    private long epoch;

    /**
     * Creates a new assignment with epoch = 1.
     */
    public static SessionAssignment initial(String sessionKey, String assignedNodeId) {
        return new SessionAssignment(sessionKey, assignedNodeId, 1L);
    }

    /**
     * Creates a new assignment with incremented epoch.
     */
    public SessionAssignment withIncrementedEpoch(String newAssignedNodeId) {
        return new SessionAssignment(sessionKey, newAssignedNodeId, epoch + 1);
    }

    /**
     * Returns true if the given node ID matches the assigned node.
     */
    public boolean isAssignedTo(String nodeId) {
        return assignedNodeId != null && assignedNodeId.equals(nodeId);
    }

    @Override
    public String toString() {
        return String.format("SessionAssignment{sessionKey='%s', assignedNodeId='%s', epoch=%d}",
                sessionKey, assignedNodeId, epoch);
    }
}