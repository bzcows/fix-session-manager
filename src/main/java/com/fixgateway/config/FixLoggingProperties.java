package com.fixgateway.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties(prefix = "fix.logging")
public class FixLoggingProperties {
    
    /**
     * Whether FIX message logging is enabled
     */
    private boolean enabled = true;
    
    /**
     * Async logging configuration
     */
    @NotNull
    private AsyncProperties async = new AsyncProperties();
    
    /**
     * Circuit breaker configuration
     */
    @NotNull
    private CircuitBreakerProperties circuitBreaker = new CircuitBreakerProperties();
    
    @Data
    public static class AsyncProperties {
        
        /**
         * Maximum capacity of the in-memory queue
         */
        @Min(1)
        private int queueCapacity = 10000;
        
        /**
         * Batch size for database writes
         */
        @Min(1)
        private int batchSize = 100;
        
        /**
         * Flush interval in milliseconds
         */
        @Min(1)
        private long flushIntervalMs = 1000;
        
        /**
         * Maximum number of retries for failed writes
         */
        @Min(0)
        private int maxRetries = 3;
        
        /**
         * Delay between retries in milliseconds
         */
        @Min(0)
        private long retryDelayMs = 1000;
        
        /**
         * Strategy to apply when queue is full
         */
        @NotNull
        private BackpressureStrategy backpressureStrategy = BackpressureStrategy.DROP_OLDEST;
    }
    
    @Data
    public static class CircuitBreakerProperties {
        
        /**
         * Whether circuit breaker is enabled
         */
        private boolean enabled = true;
        
        /**
         * Number of failures before opening the circuit
         */
        @Min(1)
        private int failureThreshold = 5;
        
        /**
         * Timeout for database operations in milliseconds
         */
        @Min(1)
        private long timeoutMs = 5000;
        
        /**
         * Time in half-open state before allowing test requests
         */
        @Min(1)
        private long halfOpenTimeoutMs = 30000;
    }
    
    /**
     * Backpressure strategies for when the queue is full
     */
    public enum BackpressureStrategy {
        /**
         * Drop the oldest message in the queue
         */
        DROP_OLDEST,
        
        /**
         * Drop the new incoming message
         */
        DROP_NEWEST,
        
        /**
         * Block until space becomes available (careful with thread blocking)
         */
        BLOCK,
        
        /**
         * Fall back to synchronous write (slower but ensures no message loss)
         */
        SYNCHRONOUS_FALLBACK
    }
}