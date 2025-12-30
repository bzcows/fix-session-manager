package com.fixgateway.component;

import com.fixgateway.model.MessageEnvelope;
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
    }

    @Override
    public void onLogout(SessionID sessionID) {
        log.info("FIX Session logged out: {}", sessionID);
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
        log.debug("Application message TO {}: {}", sessionID, message);
    }

    @Override
    public void fromApp(Message message, SessionID sessionID) 
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        
        try {
            String msgType = message.getHeader().getString(MsgType.FIELD);
            
            // Filter admin messages
            if (ADMIN_MSG_TYPES.contains(msgType)) {
                log.debug("Filtering admin message type {} from {}", msgType, sessionID);
                return;
            }

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
            
            producerTemplate.sendBody(directEndpoint, envelope);
            
            log.info("Forwarded FIX message to direct endpoint {}: {} - MsgType: {}",
                directEndpoint, sessionID, msgType);
            
        } catch (Exception e) {
            log.error("Error processing incoming FIX message from {}", sessionID, e);
        }
    }
}
