package com.fixgateway.component;

import com.fixgateway.model.MessageEnvelope;
import com.fixgateway.service.SessionAssignmentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.ProducerTemplate;
import org.springframework.stereotype.Component;
import quickfix.*;
import quickfix.field.MsgType;

import java.time.Instant;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class FixApplication implements Application {

    private final ProducerTemplate producerTemplate;
    private final org.apache.camel.CamelContext camelContext;
    private final com.fixgateway.service.SessionOwnershipService ownershipService;
    private final SessionAssignmentService sessionAssignmentService;
    
    // Admin message types to filter
    private static final Set<String> ADMIN_MSG_TYPES = Set.of(
        MsgType.HEARTBEAT,           // 0
        MsgType.TEST_REQUEST,        // 1
        MsgType.RESEND_REQUEST,      // 2
        MsgType.REJECT,              // 3
        MsgType.SEQUENCE_RESET,      // 4
        MsgType.LOGOUT,              // 5
        MsgType.LOGON                // A
    );

    @Override
    public void onCreate(SessionID sessionID) {
        log.info("FIX Session created: {}", sessionID);
    }

    @Override
    public void onLogon(SessionID sessionID) {
        log.info("FIX Session logged on: {}", sessionID);

        // Epoch fencing: ensure we are still the assigned node with correct epoch
        String assignmentKey = sessionAssignmentKey(sessionID);
        if (!sessionAssignmentService.isEpochValid(assignmentKey)) {
            log.warn("Epoch mismatch on logon for {}, stopping session", sessionID);
            // This will cause the session to stop via fencing
            return;
        }

        String sessionKey = sessionID.getSenderCompID() + "-" + sessionID.getTargetCompID();
        try {
            camelContext.getRouteController().startRoute("fix-to-kafka-" + sessionKey);
            camelContext.getRouteController().startRoute("kafka-to-fix-" + sessionKey);
            log.info("Started Kafka routes for session {}", sessionKey);
        } catch (Exception e) {
            log.error("Failed to start Kafka routes for session {}", sessionKey, e);
        }
    }

    @Override
    public void onLogout(SessionID sessionID) {
        log.info("FIX Session logged out: {}", sessionID);

        String sessionKey = sessionID.getSenderCompID() + "-" + sessionID.getTargetCompID();
        try {
            camelContext.getRouteController().stopRoute("fix-to-kafka-" + sessionKey);
            camelContext.getRouteController().stopRoute("kafka-to-fix-" + sessionKey);
            log.info("Stopped Kafka routes for session {}", sessionKey);
        } catch (Exception e) {
            log.warn("Failed to stop Kafka routes for session {}", sessionKey, e);
        }
    }

    @Override
    public void toAdmin(Message message, SessionID sessionID) {
        log.debug("Admin message TO {}: {}", sessionID, message);
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionID) 
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
        log.debug("Admin message FROM {}: {}", sessionID, message);
    }

    @Override
    public void toApp(Message message, SessionID sessionID) throws DoNotSend {
        // Epoch fencing: ensure we are still the assigned node with correct epoch
        String assignmentKey = sessionAssignmentKey(sessionID);
        if (!sessionAssignmentService.isEpochValid(assignmentKey)) {
            log.warn("Epoch mismatch on toApp for {}, throwing DoNotSend", sessionID);
            throw new DoNotSend();
        }
        log.debug("Application message TO {}: {}", sessionID, message);
    }

    @Override
    public void fromApp(Message message, SessionID sessionID)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        // Epoch fencing: ensure we are still the assigned node with correct epoch
        String assignmentKey = sessionAssignmentKey(sessionID);
        if (!sessionAssignmentService.isEpochValid(assignmentKey)) {
            log.warn("Epoch mismatch on fromApp for {}, dropping message", sessionID);
            return;
        }
        
        try {
            String msgType = message.getHeader().getString(MsgType.FIELD);
            
            // Filter admin messages
            if (ADMIN_MSG_TYPES.contains(msgType)) {
                log.debug("Filtering admin message type {} from {}", msgType, sessionID);
                return;
            }

            // Ensure routes are started before processing messages
            String sessionKey = sessionID.getSenderCompID() + "-" + sessionID.getTargetCompID();
            ensureRoutesStarted(sessionID, sessionKey);

            // Create envelope
            MessageEnvelope envelope = MessageEnvelope.builder()
                .sessionId(sessionID.toString())
                .senderCompId(sessionID.getSenderCompID())
                .targetCompId(sessionID.getTargetCompID())
                .msgType(msgType)
                .createdTimestamp(Instant.now())
                .rawMessage(message.toString())
                .build();

            // Send to direct endpoint (which routes to Kafka with proper brokers)
            String directEndpoint = String.format("direct:fix-inbound-%s-%s",
                sessionID.getSenderCompID(), sessionID.getTargetCompID());

            // Direct routes may not be fully started yet (HA takeover / async startup)
            // Retry briefly to avoid DirectConsumerNotAvailableException
            int attempts = 0;
            while (true) {
                try {
                    producerTemplate.sendBody(directEndpoint, envelope);
                    break;
                } catch (org.apache.camel.CamelExecutionException e) {
                    Throwable cause = e.getCause();
                    if (!(cause instanceof org.apache.camel.component.direct.DirectConsumerNotAvailableException)
                            || ++attempts >= 5) {
                        throw e;
                    }
                    log.debug("Route not ready, retrying ({}/5)...", attempts);
                    try {
                        Thread.sleep(200L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                }
            }
            
            log.info("Forwarded FIX message to direct endpoint {}: {} - MsgType: {}",
                directEndpoint, sessionID, msgType);
            
        } catch (Exception e) {
            log.error("Error processing incoming FIX message from {}", sessionID, e);
        }
    }

    /**
     * Ensures Camel routes are started for the given session.
     * This handles the case where messages arrive before onLogon() is called.
     */
    private void ensureRoutesStarted(SessionID sessionID, String sessionKey) {
        try {
            String fixToKafkaRoute = "fix-to-kafka-" + sessionKey;
            String kafkaToFixRoute = "kafka-to-fix-" + sessionKey;
            
            // Check if routes exist and are not started
            boolean fixToKafkaStarted = camelContext.getRouteController().getRouteStatus(fixToKafkaRoute).isStarted();
            boolean kafkaToFixStarted = camelContext.getRouteController().getRouteStatus(kafkaToFixRoute).isStarted();
            
            if (!fixToKafkaStarted || !kafkaToFixStarted) {
                log.info("Routes not started for session {}, starting now (fixToKafka={}, kafkaToFix={})",
                    sessionKey, fixToKafkaStarted, kafkaToFixStarted);
                
                if (!fixToKafkaStarted) {
                    camelContext.getRouteController().startRoute(fixToKafkaRoute);
                    log.info("Started route: {}", fixToKafkaRoute);
                }
                if (!kafkaToFixStarted) {
                    camelContext.getRouteController().startRoute(kafkaToFixRoute);
                    log.info("Started route: {}", kafkaToFixRoute);
                }
            }
        } catch (Exception e) {
            log.error("Failed to ensure routes started for session {}", sessionKey, e);
            throw new RuntimeException("Failed to start routes for session " + sessionKey, e);
        }
    }

    /**
     * Converts a SessionID to the assignment map key format.
     */
    private String sessionAssignmentKey(SessionID sessionID) {
        return sessionID.getBeginString() + ":" + sessionID.getSenderCompID() + "->" + sessionID.getTargetCompID();
    }
}
