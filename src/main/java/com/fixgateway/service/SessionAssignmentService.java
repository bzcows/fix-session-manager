package com.fixgateway.service;

import com.fixgateway.model.FixSessionConfig;
import com.fixgateway.model.FixSessionsProperties;
import com.fixgateway.model.SessionAssignment;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages deterministic assignment of FIX sessions to cluster nodes.
 * <p>
 * Only the coordinator writes assignments; all nodes watch the map and start/stop
 * sessions accordingly. Epoch fencing is enforced via the epoch field.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SessionAssignmentService {

    private final HazelcastInstance hazelcastInstance;
    private final FixSessionsProperties sessionsProperties;
    private final ClusterStabilityService clusterStabilityService;
    private final CoordinatorService coordinatorService;

    private IMap<String, SessionAssignment> assignmentMap;
    private final Map<String, Long> localEpochs = new ConcurrentHashMap<>();

    @PostConstruct
    public void initialize() {
        assignmentMap = hazelcastInstance.getMap("fix-session-assignments");
        log.info("SessionAssignmentService initialized, map size={}", assignmentMap.size());

        // Coordinator will write assignments once cluster is stable
        // Non‑coordinators remain idle

        // Start periodic logging of assignments (coordinator only)
        startAssignmentLogging();
    }

    private void startAssignmentLogging() {
        java.util.concurrent.Executors.newSingleThreadScheduledExecutor()
            .scheduleAtFixedRate(() -> {
                if (coordinatorService.isCoordinator()) {
                    logAssignments();
                }
            }, 0, 10, java.util.concurrent.TimeUnit.SECONDS);
    }

    private void logAssignments() {
        if (assignmentMap.isEmpty()) {
            log.info("Coordinator: No FIX session assignments in map");
            return;
        }
        StringBuilder sb = new StringBuilder("Coordinator: Current FIX session assignments:\n");
        for (java.util.Map.Entry<String, SessionAssignment> entry : assignmentMap.entrySet()) {
            SessionAssignment a = entry.getValue();
            sb.append(String.format("  %s -> %s (epoch=%d)\n",
                a.getSessionKey(), a.getAssignedNodeId(), a.getEpoch()));
        }
        log.info(sb.toString().trim());
    }

    /**
     * Returns the assignment map for external listeners.
     */
    public IMap<String, SessionAssignment> getAssignmentMap() {
        return assignmentMap;
    }

    /**
     * Returns the local epoch stored for a given session key.
     * Returns 0 if no local epoch is recorded.
     */
    public long getLocalEpoch(String sessionKey) {
        return localEpochs.getOrDefault(sessionKey, 0L);
    }

    /**
     * Stores the local epoch for a session.
     * Called when a session is started on this node.
     */
    public void setLocalEpoch(String sessionKey, long epoch) {
        localEpochs.put(sessionKey, epoch);
        log.debug("Local epoch set: {} -> {}", sessionKey, epoch);
    }

    /**
     * Removes local epoch (called when session is stopped).
     */
    public void removeLocalEpoch(String sessionKey) {
        localEpochs.remove(sessionKey);
        log.debug("Local epoch removed: {}", sessionKey);
    }

    /**
     * Checks whether the local epoch matches the current assignment epoch.
     * If not, the node is fenced and must stop the session.
     */
    public boolean isEpochValid(String sessionKey) {
        SessionAssignment assignment = assignmentMap.get(sessionKey);
        if (assignment == null) {
            // No assignment → not valid
            return false;
        }
        long local = getLocalEpoch(sessionKey);
        boolean valid = local == assignment.getEpoch();
        if (!valid) {
            log.warn("Epoch mismatch for {}: local={}, assignment={}", sessionKey, local, assignment.getEpoch());
        }
        return valid;
    }

    /**
     * Called by coordinator when cluster is stable.
     * Writes initial assignments (round‑robin) if the map is empty.
     * Uses startup quorum (>2 members) for safety.
     */
    public void writeInitialAssignmentsIfEmpty() {
        if (!clusterStabilityService.isStable()) {
            log.debug("Cluster not stable, skipping assignment writing");
            return;
        }
        if (!coordinatorService.isCoordinator()) {
            log.debug("Not coordinator, skipping assignment writing");
            return;
        }
        if (!assignmentMap.isEmpty()) {
            log.info("Assignment map already populated (size={}), skipping initial assignment", assignmentMap.size());
            return;
        }

        // Startup quorum: require >2 members
        if (!clusterStabilityService.hasStartupQuorum()) {
            log.warn("Startup quorum not met (need >2 members), delaying assignment writing");
            return;
        }

        List<FixSessionConfig> enabledSessions = sessionsProperties.getSessions().stream()
                .filter(FixSessionConfig::isEnabled)
                .toList();

        if (enabledSessions.isEmpty()) {
            log.warn("No enabled FIX sessions in configuration");
            return;
        }

        // Get current Hazelcast members sorted by UUID
        List<String> memberIds = hazelcastInstance.getCluster().getMembers().stream()
                .map(m -> m.getUuid().toString())
                .sorted()
                .toList();

        if (memberIds.isEmpty()) {
            log.error("No Hazelcast members found, cannot assign sessions");
            return;
        }

        log.info("Writing initial assignments for {} sessions across {} members", enabledSessions.size(), memberIds.size());

        int memberIndex = 0;
        for (FixSessionConfig config : enabledSessions) {
            String sessionKey = sessionKey(config);
            String assignedNodeId = memberIds.get(memberIndex % memberIds.size());
            SessionAssignment assignment = SessionAssignment.initial(sessionKey, assignedNodeId);

            assignmentMap.put(sessionKey, assignment);
            log.debug("Assigned {} -> {} (epoch={})", sessionKey, assignedNodeId, assignment.getEpoch());

            memberIndex++;
        }

        log.info("Initial assignments written, map size={}", assignmentMap.size());
    }

    /**
     * Reassigns all sessions currently assigned to a given node.
     * Called by coordinator when a member leaves the cluster.
     * Uses rebalance quorum (>=1 member) for safety.
     */
    public void reassignSessionsFromNode(String removedNodeId) {
        if (!clusterStabilityService.isStable()) {
            log.warn("Cluster not stable, skipping reassignment");
            return;
        }
        if (!coordinatorService.isCoordinator()) {
            log.debug("Not coordinator, skipping reassignment");
            return;
        }

        // Rebalance quorum: require >=1 member
        if (!clusterStabilityService.hasRebalanceQuorum()) {
            log.warn("Rebalance quorum not met (need >=1 member), skipping reassignment");
            return;
        }

        List<String> currentMemberIds = hazelcastInstance.getCluster().getMembers().stream()
                .map(m -> m.getUuid().toString())
                .sorted()
                .toList();

        if (currentMemberIds.isEmpty()) {
            log.error("No remaining members, cannot reassign");
            return;
        }

        // Build current load per node from existing assignments
        Map<String, Integer> loadByNode = new HashMap<>();
        for (String nodeId : currentMemberIds) {
            loadByNode.put(nodeId, 0);
        }
        for (SessionAssignment a : assignmentMap.values()) {
            loadByNode.computeIfPresent(a.getAssignedNodeId(), (k, v) -> v + 1);
        }

        int reassigned = 0;
        for (Map.Entry<String, SessionAssignment> entry : assignmentMap.entrySet()) {
            SessionAssignment assignment = entry.getValue();
            if (assignment.isAssignedTo(removedNodeId)) {
                // Choose least-busy node (tie-breaker: lexicographical order)
                String newAssignedNodeId = loadByNode.entrySet().stream()
                        .min(Comparator
                                .comparingInt(Map.Entry<String, Integer>::getValue)
                                .thenComparing(Map.Entry::getKey))
                        .map(Map.Entry::getKey)
                        .orElseThrow();

                SessionAssignment newAssignment = assignment.withIncrementedEpoch(newAssignedNodeId);
                assignmentMap.put(entry.getKey(), newAssignment);
                loadByNode.computeIfPresent(newAssignedNodeId, (k, v) -> v + 1);
                reassigned++;

                log.info("Reassigned {} from {} -> {} (epoch {} -> {})",
                        entry.getKey(), removedNodeId, newAssignedNodeId,
                        assignment.getEpoch(), newAssignment.getEpoch());
            }
        }

        if (reassigned > 0) {
            log.info("Reassigned {} sessions from removed node {}", reassigned, removedNodeId);
        }
    }

    /**
     * Converts a FIX session config to its map key.
     */
    public static String sessionKey(FixSessionConfig config) {
        return config.getFixVersion() + ":" + config.getSenderCompId() + "->" + config.getTargetCompId();
    }

    /**
     * Returns the current assignment for a session, or null if none.
     */
    public SessionAssignment getAssignment(String sessionKey) {
        return assignmentMap.get(sessionKey);
    }

    /**
     * Returns true if the given session is assigned to this node and the epoch is valid.
     */
    public boolean isAssignedToThisNode(String sessionKey) {
        SessionAssignment assignment = getAssignment(sessionKey);
        if (assignment == null) {
            return false;
        }
        return assignment.isAssignedTo(coordinatorService.localNodeId()) && isEpochValid(sessionKey);
    }
}
