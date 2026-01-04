package com.fixgateway.service;

import quickfix.Message;
import quickfix.SessionID;

import java.time.Instant;

/**
 * Interface for logging FIX messages to PostgreSQL.
 * Implementations should be non-blocking and asynchronous to avoid
 * impacting FIX message processing performance.
 */
public interface FixMessageLogger {
    
    /**
     * Log an inbound application message (from counterparty to us)
     */
    void logInboundApp(SessionID sessionId, Message message, Instant timestamp);
    
    /**
     * Log an outbound application message (from us to counterparty)
     */
    void logOutboundApp(SessionID sessionId, Message message, Instant timestamp);
    
    /**
     * Log an inbound administrative message (heartbeat, logon, etc.)
     */
    void logInboundAdmin(SessionID sessionId, Message message, Instant timestamp);
    
    /**
     * Log an outbound administrative message (heartbeat, logon, etc.)
     */
    void logOutboundAdmin(SessionID sessionId, Message message, Instant timestamp);
    
    /**
     * Check if logging is enabled
     */
    boolean isEnabled();
    
    /**
     * Get current queue size (for monitoring)
     */
    int getQueueSize();
    
    /**
     * Get number of messages successfully logged
     */
    long getLoggedCount();
    
    /**
     * Get number of messages failed to log
     */
    long getFailedCount();
    
    /**
     * Flush any pending messages to database (synchronous)
     */
    void flush();
    
    /**
     * Get circuit breaker state
     */
    String getCircuitBreakerState();
}