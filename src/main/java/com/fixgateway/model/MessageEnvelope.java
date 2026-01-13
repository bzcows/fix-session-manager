package com.fixgateway.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class MessageEnvelope {
    @JsonProperty("messageId")
    @Builder.Default
    private String messageId = UUID.randomUUID().toString();
    
    @JsonProperty("sessionId")
    private String sessionId;
    
    @JsonProperty("senderCompId")
    private String senderCompId;
    
    @JsonProperty("targetCompId")
    private String targetCompId;
    
    @JsonProperty("msgType")
    private String msgType;
    
    @JsonProperty("clOrdID")
    private String clOrdID;
    
    @JsonProperty("msgSeqNum")
    private Integer msgSeqNum;
    
    @JsonProperty("createdTimestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING,
                pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'",
                timezone = "UTC")
    private Instant createdTimestamp;
    
    @JsonProperty("rawMessage")
    private String rawMessage;
    
    @JsonProperty("messageFingerprint")
    private String messageFingerprint;
    
    @JsonProperty("kafkaTopic")
    private String kafkaTopic;
    
    @JsonProperty("kafkaPartition")
    private Integer kafkaPartition;
    
    @JsonProperty("kafkaOffset")
    private Long kafkaOffset;
    
    @JsonProperty("processingAttempt")
    @Builder.Default
    private Integer processingAttempt = 0;
    
    @JsonProperty("errorMessage")
    private String errorMessage;
    
    @JsonProperty("errorType")
    private String errorType;
    
    @JsonProperty("errorTimestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING,
                pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'",
                timezone = "UTC")
    private Instant errorTimestamp;
    
    @JsonProperty("errorRouteId")
    private String errorRouteId;
    
    /**
     * Helper method to generate a fingerprint for deduplication
     * Uses SHA-256 hash of key message components
     */
    @JsonIgnore
    public String generateFingerprint() {
        if (this.messageFingerprint != null) {
            return this.messageFingerprint;
        }
        // Create a fingerprint from key fields to identify duplicate messages
        // Includes msgSeqNum for FIX protocol-level deduplication
        String fingerprintBase = String.format("%s|%s|%s|%s|%s|%d|%s",
            sessionId != null ? sessionId : "",
            senderCompId != null ? senderCompId : "",
            targetCompId != null ? targetCompId : "",
            msgType != null ? msgType : "",
            clOrdID != null ? clOrdID : "",
            msgSeqNum != null ? msgSeqNum : 0,
            rawMessage != null ? rawMessage.hashCode() : "0"
        );
        // In a real implementation, use SHA-256: return DigestUtils.sha256Hex(fingerprintBase);
        // For now, use hash code as simple fingerprint
        this.messageFingerprint = String.valueOf(fingerprintBase.hashCode());
        return this.messageFingerprint;
    }
}
