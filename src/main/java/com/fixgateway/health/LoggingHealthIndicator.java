package com.fixgateway.health;

import com.fixgateway.metrics.LoggingMetrics;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Health indicator for the FIX message logging subsystem.
 * Provides insights into the health of the async logging queue and database connectivity.
 */
@Component
@RequiredArgsConstructor
public class LoggingHealthIndicator implements HealthIndicator {
    
    private final LoggingMetrics loggingMetrics;
    
    @Override
    public Health health() {
        // Get metrics snapshot
        LoggingMetrics.MetricsSnapshot snapshot = loggingMetrics.getSnapshot();
        
        int queueSize = snapshot.getQueueSize();
        long loggedCount = snapshot.getLoggedCount();
        long failedCount = snapshot.getFailedCount();
        String circuitBreakerState = snapshot.getCircuitBreakerState();
        
        // Determine health status based on metrics
        Health.Builder healthBuilder;
        
        if ("OPEN".equals(circuitBreakerState)) {
            // Circuit breaker is open - database connectivity issues
            healthBuilder = Health.down()
                .withDetail("status", "CIRCUIT_BREAKER_OPEN")
                .withDetail("message", "Database connectivity issues detected, circuit breaker is open");
        } else if (queueSize > 10000) { // Arbitrary threshold - could be configurable
            healthBuilder = Health.down()
                .withDetail("status", "QUEUE_OVERLOADED")
                .withDetail("message", "Logging queue is overloaded, messages may be dropped");
        } else if (failedCount > 100) { // Arbitrary threshold
            healthBuilder = Health.down()
                .withDetail("status", "HIGH_FAILURE_RATE")
                .withDetail("message", "High failure rate detected in message logging");
        } else {
            healthBuilder = Health.up()
                .withDetail("status", "HEALTHY")
                .withDetail("message", "FIX message logging is operating normally");
        }
        
        // Add detailed metrics
        return healthBuilder
            .withDetail("queue_size", queueSize)
            .withDetail("logged_count", loggedCount)
            .withDetail("failed_count", failedCount)
            .withDetail("circuit_breaker_state", circuitBreakerState)
            .withDetail("enabled", true)
            .build();
    }
}