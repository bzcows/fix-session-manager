package com.fixgateway.config;

import com.fixgateway.model.FixSessionConfig;
import com.fixgateway.model.FixSessionsProperties;
import com.fixgateway.model.PartitionStrategy;
import com.fixgateway.service.MvelExpressionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.mvel2.MVEL;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Validates MVEL partition expressions at application startup.
 * Catches syntax errors early rather than at runtime.
 */
//@Slf4j
//@Component
//@RequiredArgsConstructor
public class PartitionExpressionValidator {

/*
    private final FixSessionsProperties sessionsProperties;
    private final MvelExpressionService mvelExpressionService;

    @EventListener(ApplicationReadyEvent.class)
    public void validateExpressions() {
        log.info("Validating partition expressions for all sessions...");
        
        int validCount = 0;
        int errorCount = 0;
        
        for (FixSessionConfig config : sessionsProperties.getSessions()) {
            if (!config.isEnabled()) {
                continue;
            }
            
            PartitionStrategy strategy = config.getPartitionStrategy();
            String expression = config.getPartitionExpression();
            
            if (strategy == PartitionStrategy.NONE || expression == null || expression.trim().isEmpty()) {
                log.debug("Session {}: No partition expression to validate (strategy: {})", 
                        config.getSessionId(), strategy);
                continue;
            }
            
            try {
                validateExpression(expression, strategy, config.getInputPartitions());
                validCount++;
                log.info("Session {}: Partition expression validated successfully (strategy: {})", 
                        config.getSessionId(), strategy);
            } catch (Exception e) {
                errorCount++;
                log.error("Session {}: Invalid partition expression (strategy: {}): {}", 
                        config.getSessionId(), strategy, e.getMessage());
                log.error("Expression: {}", expression);
            }
        }
        
        if (errorCount > 0) {
            log.warn("Partition expression validation completed with {} error(s). " +
                    "Sessions with invalid expressions will use default partition assignment.", errorCount);
        } else {
            log.info("Partition expression validation completed successfully. All {} expression(s) are valid.", validCount);
        }
    }
  */  
    /**
     * Validate an MVEL expression syntax and semantics.
     */
    /*
    private void validateExpression(String expression, PartitionStrategy strategy, int totalPartitions) {
        try {
            log.debug("Validating expression for strategy {} with {} partitions: {}", strategy, totalPartitions, expression);
            
            // Preprocess expression: trim each line to handle YAML indentation issues
            String processedExpression = preprocessExpression(expression);
            if (!processedExpression.equals(expression)) {
                log.debug("Expression processed from {} chars to {} chars", expression.length(), processedExpression.length());
            }
            
            // First, compile the expression to check syntax
            Serializable compiled = MVEL.compileExpression(processedExpression);
            log.debug("Expression compiled successfully");
            
            // Create a sample context with typical FIX message fields
            Map<String, Object> sampleContext = createSampleContext();
            sampleContext.put("totalPartitions", totalPartitions);
            log.debug("Sample context created with MsgType={}", sampleContext.get("MsgType"));
            
            // Try to evaluate with sample data
            Object result = MVEL.executeExpression(compiled, sampleContext);
            log.debug("Expression evaluated, result={} (type={})", result, result != null ? result.getClass().getSimpleName() : "null");
            
            // Validate result based on strategy
            validateResult(strategy, result, totalPartitions);
            
        } catch (Exception e) {
            log.debug("Validation failed with exception: {}", e.getMessage(), e);
            throw new IllegalArgumentException("Invalid MVEL expression: " + e.getMessage(), e);
        }
    }
    */
    /**
     * Preprocess MVEL expression to handle common issues:
     * 1. Trim each line to fix YAML indentation problems
     * 2. Remove empty lines
     * 3. Ensure proper block syntax
     */
    /*
    private String preprocessExpression(String expression) {
        if (expression == null || expression.trim().isEmpty()) {
            return expression;
        }
        
        // Split into lines, trim each line, and remove empty lines
        String[] lines = expression.split("\\r?\\n");
        StringBuilder processed = new StringBuilder();
        
        for (String line : lines) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
                processed.append(trimmed).append("\n");
            }
        }
        
        // Remove trailing newline
        if (processed.length() > 0) {
            processed.setLength(processed.length() - 1);
        }
        
        return processed.toString();
    }
    */
    /**
     * Create a sample context for expression validation.
     */
    /*
    private Map<String, Object> createSampleContext() {
        Map<String, Object> context = new HashMap<>();
        
        // Sample FIX message fields
        context.put("MsgType", "D");
        context.put("ClOrdID", "12345");
        context.put("Symbol", "MSFT");
        context.put("Side", '1');
        context.put("OrderQty", 100.0);
        context.put("Price", 150.25);
        context.put("OrdType", '2');
        context.put("TimeInForce", '0');
        context.put("HandlInst", '1');
        context.put("SenderCompID", "CLIENT");
        context.put("TargetCompID", "SERVER");
        context.put("MsgSeqNum", 1);
        context.put("SendingTime", "20240101-10:30:00.000");
        context.put("rawMessage", "8=FIX.4.4|9=178|35=D|49=CLIENT|56=SERVER|34=1|52=20240101-10:30:00.000|11=12345|55=MSFT|54=1|38=100|40=2|44=150.25|59=0|60=20240101-10:30:00.000|10=000|");
        
        return context;
    }
    */
    /**
     * Validate expression result based on strategy.
     */
    /*
    private void validateResult(PartitionStrategy strategy, Object result, int totalPartitions) {
        log.debug("Validating result for strategy {}: result={}, type={}", strategy, result,
                 result != null ? result.getClass().getSimpleName() : "null");
        
        if (result == null) {
            throw new IllegalArgumentException("Expression returned null");
        }
        
        switch (strategy) {
            case KEY:
                // Any object can be a key
                log.debug("KEY strategy validation passed");
                break;
                
            case EXPR:
                // Must return an integer
                if (!(result instanceof Number)) {
                    throw new IllegalArgumentException(
                            "EXPR strategy requires integer result, got: " + result.getClass().getSimpleName());
                }
                
                int partition = ((Number) result).intValue();
                if (partition < 0 || partition >= totalPartitions) {
                    throw new IllegalArgumentException(
                            "Partition number " + partition + " is out of range [0, " + totalPartitions + ")");
                }
                log.debug("EXPR strategy validation passed: partition={} within range [0, {})", partition, totalPartitions);
                break;
                
            default:
                throw new IllegalArgumentException("Unknown partition strategy: " + strategy);
        }
    }
    */
    /**
     * Clear MVEL expression cache (useful for testing).
     */
    /*
    public void clearCache() {
        mvelExpressionService.clearCache();
        log.debug("Cleared MVEL expression cache");
    }
    */
}