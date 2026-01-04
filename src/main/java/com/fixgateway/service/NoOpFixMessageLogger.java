package com.fixgateway.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import quickfix.Message;
import quickfix.SessionID;

import java.time.Instant;

/**
 * No-op implementation of FixMessageLogger used when logging is disabled.
 * This implementation does nothing and has minimal performance impact.
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "fix.logging.enabled", havingValue = "false", matchIfMissing = false)
public class NoOpFixMessageLogger implements FixMessageLogger {
    
    @Override
    public void logInboundApp(SessionID sessionId, Message message, Instant timestamp) {
        // No-op
    }
    
    @Override
    public void logOutboundApp(SessionID sessionId, Message message, Instant timestamp) {
        // No-op
    }
    
    @Override
    public void logInboundAdmin(SessionID sessionId, Message message, Instant timestamp) {
        // No-op
    }
    
    @Override
    public void logOutboundAdmin(SessionID sessionId, Message message, Instant timestamp) {
        // No-op
    }
    
    @Override
    public boolean isEnabled() {
        return false;
    }
    
    @Override
    public int getQueueSize() {
        return 0;
    }
    
    @Override
    public long getLoggedCount() {
        return 0;
    }
    
    @Override
    public long getFailedCount() {
        return 0;
    }
    
    @Override
    public void flush() {
        // No-op
    }
    
    @Override
    public String getCircuitBreakerState() {
        return "DISABLED";
    }
}