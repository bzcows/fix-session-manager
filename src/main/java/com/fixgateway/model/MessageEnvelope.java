package com.fixgateway.model;

import com.fasterxml.jackson.annotation.JsonFormat;
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
    
    @JsonProperty("clOrdID")
    private String clOrdID;
    
    @JsonProperty("createdTimestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING,
                pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'",
                timezone = "UTC")
    private Instant createdTimestamp;
    
    @JsonProperty("rawMessage")
    private String rawMessage;
    
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
}
