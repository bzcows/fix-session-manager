package com.fixgateway.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "fix_messages")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FixMessageLog {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "session_id", nullable = false, length = 100)
    private String sessionId;
    
    @Column(name = "sender_comp_id", nullable = false, length = 50)
    private String senderCompId;
    
    @Column(name = "target_comp_id", nullable = false, length = 50)
    private String targetCompId;
    
    @Column(name = "msg_type", nullable = false, length = 10)
    private String msgType;
    
    @Column(name = "direction", nullable = false, length = 10)
    @Enumerated(EnumType.STRING)
    private MessageDirection direction;
    
    @Column(name = "message_category", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private MessageCategory category;
    
    @Column(name = "raw_message", nullable = false, columnDefinition = "TEXT")
    private String rawMessage;
    
    @Column(name = "parsed_fields", columnDefinition = "JSONB")
    private String parsedFields;
    
    @Column(name = "created_timestamp", nullable = false)
    @JsonFormat(shape = JsonFormat.Shape.STRING,
                pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'",
                timezone = "UTC")
    private Instant createdTimestamp;
    
    @Column(name = "logged_timestamp", nullable = false)
    @JsonFormat(shape = JsonFormat.Shape.STRING,
                pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'",
                timezone = "UTC")
    private Instant loggedTimestamp;
    
    @Column(name = "node_id", length = 100)
    private String nodeId;
    
    @Column(name = "sequence_number")
    private Long sequenceNumber;
    
    @Column(name = "processing_duration_ms")
    private Integer processingDurationMs;
    
    public enum MessageDirection {
        INBOUND, OUTBOUND
    }
    
    public enum MessageCategory {
        APPLICATION, ADMIN
    }
    
    /**
     * Creates a FixMessageLog from a QuickFIX/J Message and SessionID
     */
    public static FixMessageLog fromMessage(quickfix.Message message, quickfix.SessionID sessionId,
                                           MessageDirection direction, MessageCategory category,
                                           Instant timestamp, String nodeId) {
        String msgType = extractMsgType(message);
        
        return FixMessageLog.builder()
            .sessionId(sessionId.toString())
            .senderCompId(sessionId.getSenderCompID())
            .targetCompId(sessionId.getTargetCompID())
            .msgType(msgType)
            .direction(direction)
            .category(category)
            .rawMessage(message.toString())
            .parsedFields(extractParsedFields(message))
            .createdTimestamp(timestamp)
            .loggedTimestamp(Instant.now())
            .nodeId(nodeId)
            .sequenceNumber(extractSequenceNumber(message))
            .build();
    }
    
    private static String extractMsgType(quickfix.Message message) {
        try {
            return message.getHeader().getString(quickfix.field.MsgType.FIELD);
        } catch (quickfix.FieldNotFound e) {
            return "UNKNOWN";
        }
    }
    
    private static String extractParsedFields(quickfix.Message message) {
        // For now, return null. Could be enhanced to extract specific fields as JSON
        return null;
    }
    
    private static Long extractSequenceNumber(quickfix.Message message) {
        try {
            // MsgSeqNum is an integer field (tag 34)
            return (long) message.getHeader().getInt(quickfix.field.MsgSeqNum.FIELD);
        } catch (quickfix.FieldNotFound e) {
            return null;
        }
    }
}