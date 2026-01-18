package com.fixgateway.service;

import com.fixgateway.service.SessionAssignmentService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import quickfix.SessionID;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class SessionOwnershipService {

    private static final long LEASE_MILLIS = 10_000;

    private final HazelcastInstance hazelcastInstance;
    private final SessionAssignmentService sessionAssignmentService;
    private final CoordinatorService coordinatorService;
    private final ClusterStabilityService clusterStabilityService;
    /**
     * Stable node identity based on Hazelcast member UUID.
     * This MUST be used for HA correctness.
     */
    private String nodeId;
    
    private final Set<String> pendingReassignments = ConcurrentHashMap.newKeySet();
    private final Map<String, String> removedNodeAddresses = new ConcurrentHashMap<>(); // nodeId -> address

    @jakarta.annotation.PostConstruct
    void initNodeId() {
        this.nodeId = hazelcastInstance.getCluster()
            .getLocalMember()
            .getUuid()
            .toString();
        log.info("SessionOwnershipService initialized with nodeId={}", nodeId);
    }

    /**
     * Exposes local node identifier for fencing comparisons.
     */
    public String nodeId() {
        return nodeId;
    }

    private IMap<String, Ownership> ownershipMap() {
        return hazelcastInstance.getMap("fix-session-ownership");
    }

    /**
     * Register membership listener to detect crashed members and trigger takeover.
     */
    @jakarta.annotation.PostConstruct
    void registerMembershipListener() {
        hazelcastInstance.getCluster().addMembershipListener(new OwnershipMembershipListener());
        
        // Register callback for when cluster becomes stable
        clusterStabilityService.onClusterStable(() -> {
            log.info("Cluster stable callback triggered, coordinator={}, pendingReassignments={}",
                coordinatorService.isCoordinator(), pendingReassignments.size());
            
            if (!coordinatorService.isCoordinator()) {
                log.debug("Not coordinator, skipping reassignment");
                return;
            }

            for (String removedNodeId : pendingReassignments) {
                log.warn("Reassigning sessions from departed node: {}", removedNodeId);
                sessionAssignmentService.reassignSessionsFromNode(removedNodeId);
            }
            pendingReassignments.clear();
            removedNodeAddresses.clear();
        });
    }

    public boolean tryAcquire(SessionID sessionID) {
        String key = sessionID.toString();
        long now = Instant.now().toEpochMilli();

        return ownershipMap().compute(key, (k, existing) -> {
            if (existing == null || existing.leaseUntil < now) {
                log.info("Node {} acquired ownership of {}", nodeId, key);
                Ownership o = new Ownership(nodeId, now + LEASE_MILLIS);
                listeners.forEach(l -> l.onOwnershipAcquired(k));
                return o;
            }
            return existing;
        }).nodeId.equals(nodeId);
    }

    public boolean renew(SessionID sessionID) {
        String key = sessionID.toString();
        long now = Instant.now().toEpochMilli();

        return ownershipMap().computeIfPresent(key, (k, existing) -> {
            if (existing.nodeId.equals(nodeId)) {
                return new Ownership(nodeId, now + LEASE_MILLIS);
            }
            return existing;
        }) != null;
    }

    public boolean isOwner(SessionID sessionID) {
        Ownership o = ownershipMap().get(sessionID.toString());
        long now = Instant.now().toEpochMilli();
        return o != null && o.nodeId.equals(nodeId) && o.leaseUntil >= now;
    }

    /**
     * Returns current ownership record without interpreting ownership.
     * Used for lease-expiry-based fencing.
     */
    public Ownership get(SessionID sessionID) {
        return ownershipMap().get(sessionID.toString());
    }

    /**
     * Hazelcast membership listener used for deterministic FIX session failover.
     */
    private class OwnershipMembershipListener implements MembershipListener {
        @Override
        public void memberRemoved(MembershipEvent event) {
            String removedNodeId = event.getMember().getUuid().toString();
            String removedAddress = event.getMember().getAddress().toString();
            pendingReassignments.add(removedNodeId);
            removedNodeAddresses.put(removedNodeId, removedAddress);
            
            log.warn("DIAGNOSTIC: Member removed: {} (address: {}), deferring reassignment until cluster stable. pendingReassignments={}",
                removedNodeId, removedAddress, pendingReassignments.size());

            // Keep legacy lease‑based takeover for backward compatibility
            ownershipMap().forEach((sessionKey, ownership) -> {
                if (ownership.nodeId.equals(removedNodeId)) {
                    log.info("DIAGNOSTIC: Session {} owned by removed node {}, attempting takeover",
                        sessionKey, removedNodeId);
                    long now = Instant.now().toEpochMilli();
                    Ownership newOwnership = ownershipMap().compute(sessionKey, (k, existing) -> {
                        if (existing == null || existing.nodeId.equals(removedNodeId) || existing.leaseUntil < now) {
                            log.info("Node {} taking over FIX session {}", nodeId, k);
                            Ownership o = new Ownership(nodeId, now + LEASE_MILLIS);
                            listeners.forEach(l -> l.onOwnershipAcquired(k));
                            return o;
                        }
                        return existing;
                    });

                    if (newOwnership.nodeId.equals(nodeId)) {
                        onOwnershipAcquired(sessionKey);
                    }
                }
            });
        }

        @Override
        public void memberAdded(MembershipEvent event) {
            String addedNodeId = event.getMember().getUuid().toString();
            String addedAddress = event.getMember().getAddress().toString();
            log.info("DIAGNOSTIC: Member added: {} (address: {}), total members now: {}",
                addedNodeId, addedAddress, hazelcastInstance.getCluster().getMembers().size());
            
            // Check if a node previously removed from this same address is in pendingReassignments
            // When a node restarts, it gets a new UUID but same address
            // We should not reassign sessions from a node that has rejoined
            for (Map.Entry<String, String> entry : removedNodeAddresses.entrySet()) {
                String oldNodeId = entry.getKey();
                String oldAddress = entry.getValue();
                if (oldAddress.equals(addedAddress)) {
                    log.warn("DIAGNOSTIC: Node rejoined at same address {} (old UUID: {}, new UUID: {}), removing old UUID from pendingReassignments",
                        addedAddress, oldNodeId, addedNodeId);
                    pendingReassignments.remove(oldNodeId);
                    removedNodeAddresses.remove(oldNodeId);
                    break;
                }
            }
            
            // Also check if the exact UUID is in pendingReassignments (for completeness)
            if (pendingReassignments.contains(addedNodeId)) {
                log.warn("DIAGNOSTIC: Rejoined node {} was in pendingReassignments, removing", addedNodeId);
                pendingReassignments.remove(addedNodeId);
                removedNodeAddresses.remove(addedNodeId);
            }
        }
    }

    /**
     * Hook for FIX session startup on ownership acquisition.
     * Implemented by FixSessionManager via listener wiring.
     */
    protected void onOwnershipAcquired(String sessionKey) {
        log.info("Ownership acquired for FIX session {} on node {}", sessionKey, nodeId);
    }

    /**
     * Simple ownership value object (avoid Java preview features).
     */
    static class Ownership {
        final String nodeId;
        final long leaseUntil;
        Ownership(String nodeId, long leaseUntil) {
            this.nodeId = nodeId;
            this.leaseUntil = leaseUntil;
        }
    }

    /**
     * Listener interface for ownership lifecycle events.
     */
    public interface OwnershipListener {
        void onOwnershipAcquired(String sessionKey);
        void onOwnershipLost(String sessionKey);
    }

    private final java.util.List<OwnershipListener> listeners = new java.util.concurrent.CopyOnWriteArrayList<>();

    public void registerListener(OwnershipListener listener) {
        listeners.add(listener);
    }
}
