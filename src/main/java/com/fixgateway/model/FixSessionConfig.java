package com.fixgateway.model;

import lombok.Data;

@Data
public class FixSessionConfig {
    private String sessionId;
    private String type; // ACCEPTOR or INITIATOR
    private String senderCompId;
    private String targetCompId;
    private String host;
    private int port;
    private String fixVersion;
    private String dictionary;
    private boolean enabled;
    private int heartbeatInterval;
    private boolean resetOnLogon;
    private boolean resetOnLogout;
    private boolean resetOnDisconnect;
    
    // Content-based routing configuration
    private int inputPartitions = 1;
    private PartitionStrategy partitionStrategy = PartitionStrategy.NONE;
    private String partitionExpression;
}
