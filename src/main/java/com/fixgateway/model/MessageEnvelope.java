package com.fixgateway.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageEnvelope {
    @JsonProperty("sessionId")
    private String sessionId;
    
    @JsonProperty("senderCompId")
    private String senderCompId;
    
    @JsonProperty("targetCompId")
    private String targetCompId;
    
    @JsonProperty("msgType")
    private String msgType;
    
    @JsonProperty("createdTimestamp")
    private Instant createdTimestamp;
    
    @JsonProperty("rawMessage")
    private String rawMessage;
}
