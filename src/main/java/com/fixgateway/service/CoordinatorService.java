package com.fixgateway.service;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.core.HazelcastInstance;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Determines which node is the coordinator (lowest Hazelcast member UUID).
 * <p>
 * Coordinator election is deterministic and recomputed on every membership change.
 * This service has no side effects; it only answers queries.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CoordinatorService {

    private final HazelcastInstance hazelcastInstance;

    private final AtomicReference<String> coordinatorNodeId = new AtomicReference<>(null);
    private final AtomicReference<String> localNodeId = new AtomicReference<>(null);

    @PostConstruct
    public void initialize() {
        // Store local node ID for fast comparison
        localNodeId.set(hazelcastInstance.getCluster().getLocalMember().getUuid().toString());

        // Register membership listener to recompute coordinator on changes
        hazelcastInstance.getCluster().addMembershipListener(new CoordinatorMembershipListener());

        // Initial computation
        recomputeCoordinator();
        log.info("CoordinatorService initialized, localNodeId={}, coordinatorNodeId={}",
                localNodeId.get(), coordinatorNodeId.get());
    }

    /**
     * Returns true if this node is the current coordinator.
     */
    public boolean isCoordinator() {
        String coord = coordinatorNodeId.get();
        String local = localNodeId.get();
        return coord != null && coord.equals(local);
    }

    /**
     * Returns the UUID of the current coordinator node.
     * Returns null if the cluster is empty (should never happen in practice).
     */
    public String coordinatorNodeId() {
        return coordinatorNodeId.get();
    }

    /**
     * Returns the local node's UUID.
     */
    public String localNodeId() {
        return localNodeId.get();
    }

    /**
     * Recomputes coordinator based on current Hazelcast members.
     * Coordinator = member with lowest UUID (lexicographically).
     */
    private void recomputeCoordinator() {
        java.util.Set<Member> members = hazelcastInstance.getCluster().getMembers();
        if (members.isEmpty()) {
            log.warn("No Hazelcast members found, cannot elect coordinator");
            coordinatorNodeId.set(null);
            return;
        }

        Member lowest = members.stream()
                .min(Comparator.comparing(m -> m.getUuid().toString()))
                .orElseThrow();

        String newCoordinatorId = lowest.getUuid().toString();
        String previous = coordinatorNodeId.getAndSet(newCoordinatorId);

        if (!newCoordinatorId.equals(previous)) {
            log.info("Coordinator changed: {} -> {} (total members={})",
                    previous, newCoordinatorId, members.size());
        }
    }

    private class CoordinatorMembershipListener implements MembershipListener {
        @Override
        public void memberAdded(MembershipEvent event) {
            onMembershipChange();
        }

        @Override
        public void memberRemoved(MembershipEvent event) {
            onMembershipChange();
        }

        private void onMembershipChange() {
            recomputeCoordinator();
        }
    }
}