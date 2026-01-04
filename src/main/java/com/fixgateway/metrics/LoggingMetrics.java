package com.fixgateway.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector for FIX message logging subsystem.
 * Exposes metrics to Micrometer for monitoring and alerting.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LoggingMetrics {
    
    private final MeterRegistry meterRegistry;
    
    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final AtomicLong loggedCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);
    private final AtomicLong droppedCount = new AtomicLong(0);
    private final AtomicInteger circuitBreakerState = new AtomicInteger(0); // 0=CLOSED, 1=OPEN, 2=HALF_OPEN, 3=DISABLED
    private final AtomicInteger queueUtilization = new AtomicInteger(0); // 0-100 percentage
    
    private Timer writeTimer;
    
    @PostConstruct
    public void init() {
        // Initialize timer
        writeTimer = Timer.builder("fix.logging.write.duration")
            .description("Duration of database write operations")
            .register(meterRegistry);
        
        // Register gauges for real-time metrics
        Gauge.builder("fix.logging.queue.size", queueSize, AtomicInteger::get)
            .description("Current size of the logging queue")
            .register(meterRegistry);
        
        Gauge.builder("fix.logging.queue.utilization", queueUtilization, AtomicInteger::get)
            .description("Queue utilization percentage (0-100)")
            .register(meterRegistry);
        
        Gauge.builder("fix.logging.messages.logged", loggedCount, AtomicLong::get)
            .description("Total number of messages successfully logged")
            .register(meterRegistry);
        
        Gauge.builder("fix.logging.messages.failed", failedCount, AtomicLong::get)
            .description("Total number of messages that failed to log")
            .register(meterRegistry);
        
        Gauge.builder("fix.logging.messages.dropped", droppedCount, AtomicLong::get)
            .description("Total number of messages dropped due to backpressure")
            .register(meterRegistry);
        
        Gauge.builder("fix.logging.circuit.breaker.state", circuitBreakerState, AtomicInteger::get)
            .description("Circuit breaker state (0=CLOSED, 1=OPEN, 2=HALF_OPEN, 3=DISABLED)")
            .register(meterRegistry);
        
        log.info("Logging metrics initialized");
    }
    
    /**
     * Update queue size metric
     */
    public void updateQueueSize(int size) {
        queueSize.set(size);
    }
    
    /**
     * Update queue utilization percentage (0-100)
     */
    public void updateQueueUtilization(int utilization) {
        queueUtilization.set(Math.min(100, Math.max(0, utilization)));
    }
    
    /**
     * Update circuit breaker state
     */
    public void updateCircuitBreakerState(String state) {
        int value;
        switch (state) {
            case "CLOSED":
                value = 0;
                break;
            case "OPEN":
                value = 1;
                break;
            case "HALF_OPEN":
                value = 2;
                break;
            case "DISABLED":
                value = 3;
                break;
            default:
                value = 4; // UNKNOWN
        }
        circuitBreakerState.set(value);
    }
    
    /**
     * Increment logged messages counter
     */
    public void incrementLoggedCount(long count) {
        loggedCount.addAndGet(count);
    }
    
    /**
     * Increment failed messages counter
     */
    public void incrementFailedCount(long count) {
        failedCount.addAndGet(count);
    }
    
    /**
     * Increment dropped messages counter
     */
    public void incrementDroppedCount(long count) {
        droppedCount.addAndGet(count);
    }
    
    /**
     * Record database write duration
     */
    public void recordWriteDuration(long duration, TimeUnit unit) {
        writeTimer.record(duration, unit);
    }
    
    /**
     * Get all current metrics as a snapshot
     */
    public MetricsSnapshot getSnapshot() {
        return MetricsSnapshot.builder()
            .queueSize(queueSize.get())
            .loggedCount(loggedCount.get())
            .failedCount(failedCount.get())
            .droppedCount(droppedCount.get())
            .writeTimerMean(writeTimer.mean(TimeUnit.MILLISECONDS))
            .writeTimerMax(writeTimer.max(TimeUnit.MILLISECONDS))
            .circuitBreakerState(getCircuitBreakerStateString())
            .build();
    }
    
    /**
     * Get circuit breaker state as string
     */
    private String getCircuitBreakerStateString() {
        int state = circuitBreakerState.get();
        switch (state) {
            case 0:
                return "CLOSED";
            case 1:
                return "OPEN";
            case 2:
                return "HALF_OPEN";
            case 3:
                return "DISABLED";
            default:
                return "UNKNOWN";
        }
    }
    
    @lombok.Builder
    @lombok.Data
    public static class MetricsSnapshot {
        private int queueSize;
        private long loggedCount;
        private long failedCount;
        long droppedCount;
        private double writeTimerMean;
        private double writeTimerMax;
        private String circuitBreakerState;
    }
}