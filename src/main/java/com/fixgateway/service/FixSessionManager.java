package com.fixgateway.service;

import com.fixgateway.component.FixApplication;
import com.fixgateway.model.FixSessionConfig;
import com.fixgateway.model.FixSessionsProperties;
import com.fixgateway.store.HazelcastLogFactory;
import com.fixgateway.store.HazelcastMessageStoreFactory;
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
    
    private final List<Acceptor> acceptors = new ArrayList<>();
    private final List<Initiator> initiators = new ArrayList<>();

    @PostConstruct
    public void initialize() {
        log.info("Initializing FIX Sessions...");
        
        for (FixSessionConfig config : sessionsProperties.getSessions()) {
            if (!config.isEnabled()) {
                log.info("Session {} is disabled, skipping", config.getSessionId());
                continue;
            }

            try {
                SessionSettings settings = createSessionSettings(config);
                
                if ("ACCEPTOR".equalsIgnoreCase(config.getType())) {
                    startAcceptor(config, settings);
                } else if ("INITIATOR".equalsIgnoreCase(config.getType())) {
                    startInitiator(config, settings);
                } else {
                    log.error("Unknown session type: {}", config.getType());
                }
            } catch (Exception e) {
                log.error("Failed to start session: {}", config.getSessionId(), e);
            }
        }
        
        log.info("FIX Session initialization complete. Acceptors: {}, Initiators: {}", 
            acceptors.size(), initiators.size());
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
        log.info("Starting ACCEPTOR session: {}", config.getSessionId());
        
        SocketAcceptor acceptor = new SocketAcceptor(
            fixApplication,
            messageStoreFactory,
            settings,
            logFactory,
            new DefaultMessageFactory()
        );
        
        acceptor.start();
        acceptors.add(acceptor);
        
        log.info("ACCEPTOR session started: {} on {}:{}", 
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
