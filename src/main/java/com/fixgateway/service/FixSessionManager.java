package com.fixgateway.service;

import com.fixgateway.component.FixApplication;
import com.fixgateway.model.FixSessionConfig;
import com.fixgateway.model.FixSessionsProperties;
import com.fixgateway.model.SessionAssignment;
import com.fixgateway.store.HazelcastLogFactory;
import com.fixgateway.store.HazelcastMessageStoreFactory;
import com.hazelcast.core.EntryEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import quickfix.*;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class FixSessionManager {

    private final FixSessionsProperties sessionsProperties;
    private final FixApplication fixApplication;
    private final HazelcastMessageStoreFactory messageStoreFactory;
    private final HazelcastLogFactory logFactory;
    private final ClusterStabilityService clusterStabilityService;
    private final CoordinatorService coordinatorService;
    private final SessionAssignmentService sessionAssignmentService;
    
    private final List<Acceptor> acceptors = new ArrayList<>();
    private final List<Initiator> initiators = new ArrayList<>();
    /**
     * Tracks FIX sessions currently active on this node to prevent duplicate startup
     * from lease monitor + membership events.
     */
    private final java.util.Set<SessionID> activeSessions =
        java.util.Collections.synchronizedSet(new java.util.HashSet<>());

    @jakarta.annotation.PostConstruct
    public void startLeaseMonitor() {
        // Lease monitor is now ONLY responsible for renewals and fencing.
        // Session startup is driven by ownership events.
        java.util.concurrent.Executors.newSingleThreadScheduledExecutor()
            .scheduleAtFixedRate(this::monitorOwnership, 3, 3, java.util.concurrent.TimeUnit.SECONDS);
    }

    private void monitorOwnership() {
        // Epoch‑based fencing: stop any session where local epoch != assignment epoch
        for (SessionID sessionID : activeSessions) {
            String sessionKey = SessionAssignmentService.sessionKey(findConfigBySessionId(sessionID));
            if (sessionKey != null && !sessionAssignmentService.isEpochValid(sessionKey)) {
                log.warn("Epoch mismatch detected for {}, stopping session", sessionID);
                stopSession(sessionID);
                sessionAssignmentService.removeLocalEpoch(sessionKey);
            }
        }
    }

    private FixSessionConfig findConfigBySessionId(SessionID sessionID) {
        for (FixSessionConfig cfg : sessionsProperties.getSessions()) {
            if (cfg.getFixVersion().equals(sessionID.getBeginString())
                    && cfg.getSenderCompId().equals(sessionID.getSenderCompID())
                    && cfg.getTargetCompId().equals(sessionID.getTargetCompID())) {
                return cfg;
            }
        }
        return null;
    }

    private void startSessionForConfig(FixSessionConfig config) {
        try {
            SessionID sessionID = new SessionID(
                config.getFixVersion(),
                config.getSenderCompId(),
                config.getTargetCompId());

            if (!activeSessions.add(sessionID)) {
                log.info("FIX session {} already active on this node, skipping start", sessionID);
                return;
            }

            SessionSettings settings = createSessionSettings(config);
            if ("ACCEPTOR".equalsIgnoreCase(config.getType())) {
                startAcceptor(config, settings);
            } else if ("INITIATOR".equalsIgnoreCase(config.getType())) {
                startInitiator(config, settings);
            }
        } catch (Exception e) {
            log.error("Failed to start FIX session on takeover: {}", config.getSessionId(), e);
        }
    }

    private void stopSession(SessionID sessionID) {
        activeSessions.remove(sessionID);
        // Stop ONLY acceptors/initiators that correspond to this session
        acceptors.removeIf(a -> {
            boolean matches = a.getSessions().contains(sessionID);
            if (matches) {
                try {
                    a.stop();
                } catch (Exception e) {
                    log.warn("Error stopping acceptor for {}", sessionID, e);
                }
            }
            return matches;
        });
        initiators.removeIf(i -> {
            boolean matches = i.getSessions().contains(sessionID);
            if (matches) {
                try {
                    i.stop();
                } catch (Exception e) {
                    log.warn("Error stopping initiator for {}", sessionID, e);
                }
            }
            return matches;
        });
    }

    @PostConstruct
    public void initialize() {
        log.info("Initializing FIX Sessions...");

        // 1. Wait for cluster stability before any action
        if (!clusterStabilityService.isStable()) {
            log.info("Cluster not yet stable, deferring FIX session initialization");
            // Schedule a retry check after a short delay
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor()
                .schedule(this::initialize, 5, java.util.concurrent.TimeUnit.SECONDS);
            return;
        }

        // 2. Coordinator writes initial assignments if map is empty
        if (coordinatorService.isCoordinator()) {
            sessionAssignmentService.writeInitialAssignmentsIfEmpty();
        }

        // 3. Register assignment map listener to react to assignments
        registerAssignmentMapListener();

        // 4. Process any existing assignments (in case we joined after assignments were written)
        processExistingAssignments();

        log.info("FIX Session initialization complete. Acceptors: {}, Initiators: {}",
            acceptors.size(), initiators.size());
    }

    private FixSessionConfig findConfig(String sessionKey) {
        for (FixSessionConfig cfg : sessionsProperties.getSessions()) {
            String key = cfg.getFixVersion() + ":" + cfg.getSenderCompId() + "->" + cfg.getTargetCompId();
            if (key.equals(sessionKey)) {
                return cfg;
            }
        }
        return null;
    }

    private void registerAssignmentMapListener() {
        sessionAssignmentService.getAssignmentMap().addEntryListener(
            new com.hazelcast.map.listener.EntryAddedListener<Object, Object>() {
                @Override
                public void entryAdded(EntryEvent<Object, Object> event) {
                    onAssignmentChanged((String) event.getKey(), (SessionAssignment) event.getValue());
                }
            },
            true
        );
        sessionAssignmentService.getAssignmentMap().addEntryListener(
            new com.hazelcast.map.listener.EntryUpdatedListener<Object, Object>() {
                @Override
                public void entryUpdated(EntryEvent<Object, Object> event) {
                    onAssignmentChanged((String) event.getKey(), (SessionAssignment) event.getValue());
                }
            },
            true
        );
        sessionAssignmentService.getAssignmentMap().addEntryListener(
            new com.hazelcast.map.listener.EntryRemovedListener<Object, Object>() {
                @Override
                public void entryRemoved(EntryEvent<Object, Object> event) {
                    onAssignmentRemoved((String) event.getKey());
                }
            },
            true
        );
        log.info("Assignment map listener registered");
    }

    private void onAssignmentChanged(String sessionKey, SessionAssignment assignment) {
        String localNodeId = coordinatorService.localNodeId();
        if (assignment.isAssignedTo(localNodeId)) {
            // This node is now assigned
            log.info("Assignment detected for {} (epoch={}), starting FIX session", sessionKey, assignment.getEpoch());
            sessionAssignmentService.setLocalEpoch(sessionKey, assignment.getEpoch());
            FixSessionConfig config = findConfig(sessionKey);
            if (config != null) {
                startSessionForConfig(config);
            } else {
                log.warn("No config found for assigned session key: {}", sessionKey);
            }
        } else {
            // This node is NOT assigned → stop if we were running it
            log.debug("Assignment changed for {}, not assigned to this node, stopping if active", sessionKey);
            FixSessionConfig config = findConfig(sessionKey);
            if (config != null) {
                SessionID sessionID = new SessionID(config.getFixVersion(),
                    config.getSenderCompId(), config.getTargetCompId());
                stopSession(sessionID);
                sessionAssignmentService.removeLocalEpoch(sessionKey);
            }
        }
    }

    private void onAssignmentRemoved(String sessionKey) {
        log.info("Assignment removed for {}, stopping session", sessionKey);
        FixSessionConfig config = findConfig(sessionKey);
        if (config != null) {
            SessionID sessionID = new SessionID(config.getFixVersion(),
                config.getSenderCompId(), config.getTargetCompId());
            stopSession(sessionID);
            sessionAssignmentService.removeLocalEpoch(sessionKey);
        }
    }

    private void processExistingAssignments() {
        for (FixSessionConfig config : sessionsProperties.getSessions()) {
            if (!config.isEnabled()) {
                continue;
            }
            String sessionKey = SessionAssignmentService.sessionKey(config);
            SessionAssignment assignment = sessionAssignmentService.getAssignment(sessionKey);
            if (assignment != null && assignment.isAssignedTo(coordinatorService.localNodeId())) {
                log.info("Existing assignment found for {} (epoch={}), starting session",
                    sessionKey, assignment.getEpoch());
                sessionAssignmentService.setLocalEpoch(sessionKey, assignment.getEpoch());
                startSessionForConfig(config);
            }
        }
    }

    private SessionSettings createSessionSettings(FixSessionConfig config) throws ConfigError {
        SessionSettings settings = new SessionSettings();
        SessionID sessionID = new SessionID(config.getFixVersion(),
            config.getSenderCompId(), config.getTargetCompId());

        log.debug("Creating session settings for {}: {}", config.getSessionId(), sessionID);
        
        // Set ConnectionType (required by QuickFIX)
        if ("ACCEPTOR".equalsIgnoreCase(config.getType())) {
            settings.setString(sessionID, "ConnectionType", "acceptor");
            settings.setString(sessionID, Acceptor.SETTING_SOCKET_ACCEPT_ADDRESS, config.getHost());
            settings.setLong(sessionID, Acceptor.SETTING_SOCKET_ACCEPT_PORT, config.getPort());
        } else if ("INITIATOR".equalsIgnoreCase(config.getType())) {
            settings.setString(sessionID, "ConnectionType", "initiator");
            settings.setString(sessionID, Initiator.SETTING_SOCKET_CONNECT_HOST, config.getHost());
            settings.setLong(sessionID, Initiator.SETTING_SOCKET_CONNECT_PORT, config.getPort());
            settings.setBool(sessionID, Initiator.SETTING_RECONNECT_INTERVAL, true);
            settings.setLong(sessionID, Initiator.SETTING_RECONNECT_INTERVAL, 5);
        }
        
        // Common session settings
        settings.setString(sessionID, SessionSettings.BEGINSTRING, config.getFixVersion());
        settings.setString(sessionID, SessionSettings.SENDERCOMPID, config.getSenderCompId());
        settings.setString(sessionID, SessionSettings.TARGETCOMPID, config.getTargetCompId());
        settings.setString(sessionID, Session.SETTING_START_TIME, "00:00:00");
        settings.setString(sessionID, Session.SETTING_END_TIME, "00:00:00");
        settings.setLong(sessionID, Session.SETTING_HEARTBTINT, config.getHeartbeatInterval());
        // IMPORTANT: Disable automatic resets to allow crash-recovery using persisted MessageStore
        // Any reset here will invoke MessageStore.reset() and force MsgSeqNum back to 1
        settings.setBool(sessionID, Session.SETTING_RESET_ON_LOGON, false);
        settings.setBool(sessionID, Session.SETTING_RESET_ON_LOGOUT, false);
        settings.setBool(sessionID, Session.SETTING_RESET_ON_DISCONNECT, false);
        
        // Log the settings for debugging
        log.debug("Session settings created for {}: ConnectionType={}, Host={}, Port={}",
            config.getSessionId(), config.getType(), config.getHost(), config.getPort());

        return settings;
    }

    private void startAcceptor(FixSessionConfig config, SessionSettings settings) throws ConfigError {
        log.info("Starting THREADED ACCEPTOR session: {}", config.getSessionId());
        
        ThreadedSocketAcceptor acceptor = new ThreadedSocketAcceptor(
            fixApplication,
            messageStoreFactory,
            settings,
            logFactory,
            new DefaultMessageFactory()
        );
        
        acceptor.start();
        acceptors.add(acceptor);
        
        log.info("THREADED ACCEPTOR session started: {} on {}:{}",
            config.getSessionId(), config.getHost(), config.getPort());
    }

    private void startInitiator(FixSessionConfig config, SessionSettings settings) throws ConfigError {
        log.info("Starting INITIATOR session: {}", config.getSessionId());
        
        SocketInitiator initiator = new SocketInitiator(
            fixApplication,
            messageStoreFactory,
            settings,
            logFactory,
            new DefaultMessageFactory()
        );
        
        initiator.start();
        initiators.add(initiator);
        
        log.info("INITIATOR session started: {} connecting to {}:{}", 
            config.getSessionId(), config.getHost(), config.getPort());
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down FIX sessions...");
        
        acceptors.forEach(acceptor -> {
            try {
                acceptor.stop();
            } catch (Exception e) {
                log.error("Error stopping acceptor", e);
            }
        });
        
        initiators.forEach(initiator -> {
            try {
                initiator.stop();
            } catch (Exception e) {
                log.error("Error stopping initiator", e);
            }
        });
        
        log.info("FIX sessions shutdown complete");
    }
}
