package com.fixgateway.service;

import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.core.HazelcastInstance;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Ensures the Hazelcast cluster is stable before any FIX or Kafka activity begins.
 * <p>
 * Stability is defined as: no membership changes for at least 30 seconds.
 * <p>
 * This service provides a hard gate that other components must respect.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ClusterStabilityService {

    private final HazelcastInstance hazelcastInstance;

    private final AtomicReference<State> currentState = new AtomicReference<>(State.STARTING);
    private volatile long lastMembershipChangeTime = 0L;
    private ScheduledExecutorService scheduler;
    private Runnable onStableCallback;

    private static final long STABILITY_WINDOW_MS = 30_000L; // 30 seconds
    private static final int STABILITY_QUORUM = 2; // Minimum members for startup stability
    private static final int REBALANCE_QUORUM = 1; // Minimum members for rebalance

    public enum State {
        STARTING,
        STABILIZING,
        STABLE
    }

    @PostConstruct
    public void initialize() {
        log.info("ClusterStabilityService initializing");
        lastMembershipChangeTime = System.currentTimeMillis();

        // Register membership listener
        hazelcastInstance.getCluster().addMembershipListener(new StabilityMembershipListener());

        // Start periodic stability checker
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-stability-checker");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(this::checkStability, 1, 1, TimeUnit.SECONDS);
        log.info("ClusterStabilityService started, waiting for cluster to stabilize");
    }

    @PreDestroy
    public void shutdown() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /**
     * Returns true if the cluster has been stable for at least 30 seconds.
     * This is the primary gate that other services must check before taking any action.
     */
    public boolean isStable() {
        return currentState.get() == State.STABLE;
    }

    /**
     * Returns true if the cluster meets the startup stability quorum (>2 members).
     * Used only during initial node startup.
     */
    public boolean hasStartupQuorum() {
        int memberCount = hazelcastInstance.getCluster().getMembers().size();
        return memberCount > STABILITY_QUORUM;
    }

    /**
     * Returns true if the cluster meets the rebalance quorum (>=1 member).
     * Used during rebalance operations.
     */
    public boolean hasRebalanceQuorum() {
        int memberCount = hazelcastInstance.getCluster().getMembers().size();
        return memberCount >= REBALANCE_QUORUM;
    }

    /**
     * Returns the current state for monitoring.
     */
    public State getState() {
        return currentState.get();
    }

    /**
     * Returns the timestamp of the last membership change.
     */
    public long getLastMembershipChangeTime() {
        return lastMembershipChangeTime;
    }

    /**
     * Register a callback to be invoked when the cluster becomes stable.
     * The callback will be invoked each time the cluster transitions to STABLE state.
     */
    public void onClusterStable(Runnable callback) {
        this.onStableCallback = callback;
    }

    /**
     * Blocking wait for stability (optional).
     * @param timeoutMs maximum time to wait
     * @return true if stable within timeout, false otherwise
     */
    public boolean awaitStability(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (isStable()) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    private void checkStability() {
        long now = System.currentTimeMillis();
        long timeSinceLastChange = now - lastMembershipChangeTime;

        State current = currentState.get();

        if (current == State.STARTING) {
            // Transition to STABILIZING immediately after first check
            currentState.compareAndSet(State.STARTING, State.STABILIZING);
            log.debug("Cluster entered STABILIZING state");
            return;
        }

        if (current == State.STABILIZING && timeSinceLastChange >= STABILITY_WINDOW_MS) {
            if (currentState.compareAndSet(State.STABILIZING, State.STABLE)) {
                log.info("Cluster is now STABLE (no membership changes for {} ms)", STABILITY_WINDOW_MS);
                // Invoke the stable callback if registered
                if (onStableCallback != null) {
                    try {
                        onStableCallback.run();
                    } catch (Exception e) {
                        log.error("Error executing stable callback", e);
                    }
                }
            }
            return;
        }

        // If we are STABLE and a membership change occurs, revert to STABILIZING
        if (current == State.STABLE && timeSinceLastChange < STABILITY_WINDOW_MS) {
            if (currentState.compareAndSet(State.STABLE, State.STABILIZING)) {
                log.warn("Cluster lost stability due to recent membership change, returning to STABILIZING");
            }
        }
    }

    private class StabilityMembershipListener implements MembershipListener {
        @Override
        public void memberAdded(MembershipEvent event) {
            onMembershipChange();
        }

        @Override
        public void memberRemoved(MembershipEvent event) {
            onMembershipChange();
        }

        private void onMembershipChange() {
            long now = System.currentTimeMillis();
            lastMembershipChangeTime = now;

            State current = currentState.get();
            if (current == State.STABLE) {
                // Already stable → revert to STABILIZING
                currentState.compareAndSet(State.STABLE, State.STABILIZING);
                log.info("Membership changed, cluster stability reset");
            } else if (current == State.STARTING) {
                // Still starting, keep as STARTING
            } else {
                // Already STABILIZING, no state change needed
            }

            log.debug("Membership change detected, lastChangeTime={}", Instant.ofEpochMilli(now));
        }
    }
}