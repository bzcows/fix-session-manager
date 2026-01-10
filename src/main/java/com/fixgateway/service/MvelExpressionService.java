package com.fixgateway.service;

import com.fixgateway.model.PartitionStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.mvel2.MVEL;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for evaluating MVEL expressions for content-based routing.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MvelExpressionService {

    private final FixMessageParser fixMessageParser;
    
    // Cache compiled expressions for better performance
    private final Map<String, Serializable> expressionCache = new ConcurrentHashMap<>();
    
    /**
     * Evaluate a partition expression based on the strategy and FIX message.
     * 
     * @param strategy The partition strategy (NONE, KEY, EXPR)
     * @param expression The MVEL expression (can be null for NONE strategy)
     * @param rawFixMessage The raw FIX message string
     * @param totalPartitions Total number of partitions for the topic
     * @return Object representing the partition key or partition number
     */
    public Object evaluatePartition(
            PartitionStrategy strategy,
            String expression,
            String rawFixMessage,
            int totalPartitions) {
        
        if (strategy == PartitionStrategy.NONE || expression == null || expression.trim().isEmpty()) {
            log.debug("No partition expression to evaluate (strategy: {})", strategy);
            return null;
        }
        
        try {
            // Parse FIX message to create evaluation context
            Map<String, Object> context = fixMessageParser.parseFixMessage(rawFixMessage);
            
            // Add helper variables to context
            context.put("totalPartitions", totalPartitions);
            
            // Evaluate the expression
            Object result = evaluateExpression(expression, context);
            
            if (result == null) {
                log.warn("MVEL expression returned null for strategy: {}", strategy);
                return null;
            }
            
            log.debug("MVEL expression evaluated successfully. Strategy: {}, Result: {}, Type: {}", 
                     strategy, result, result.getClass().getSimpleName());
            
            // Validate result based on strategy
            return validateResult(strategy, result, totalPartitions);
            
        } catch (Exception e) {
            log.warn("Failed to evaluate MVEL expression for strategy {}: {}", strategy, e.getMessage());
            return null;
        }
    }
    
    /**
     * Evaluate a partition expression with a pre-parsed QuickFIX/J Message object.
     *
     * @param strategy The partition strategy (NONE, KEY, EXPR)
     * @param expression The MVEL expression (can be null for NONE strategy)
     * @param fixMessage The QuickFIX/J Message object
     * @param totalPartitions Total number of partitions for the topic
     * @return Object representing the partition key or partition number
     */
    public Object evaluatePartition(
            PartitionStrategy strategy,
            String expression,
            quickfix.Message fixMessage,
            int totalPartitions) {
        
        if (strategy == PartitionStrategy.NONE || expression == null || expression.trim().isEmpty()) {
            log.info("No partition expression to evaluate (strategy: {})", strategy);
            return null;
        }
        
        try {
            log.info("Parsing FIX message for MVEL evaluation: strategy={}, expressionLength={}, totalPartitions={}",
                strategy, expression.length(), totalPartitions);
            
            // Parse FIX message to create evaluation context
            Map<String, Object> context = fixMessageParser.parseFixMessage(fixMessage);
            
            // Log context keys for debugging
            log.info("MVEL evaluation context keys: {}", context.keySet());
            if (context.containsKey("MsgType")) {
                log.info("MsgType in context: {}", context.get("MsgType"));
            }
            
            // Add helper variables to context
            context.put("totalPartitions", totalPartitions);
            
            // Evaluate the expression
            Object result = evaluateExpression(expression, context);
            
            if (result == null) {
                log.warn("MVEL expression returned null for strategy: {}", strategy);
                return null;
            }
            
            log.info("MVEL expression evaluated successfully. Strategy: {}, Result: {}, Type: {}",
                     strategy, result, result.getClass().getSimpleName());
            
            // Validate result based on strategy
            Object validatedResult = validateResult(strategy, result, totalPartitions);
            log.info("Validated result: {} (original: {})", validatedResult, result);
            
            return validatedResult;
            
        } catch (Exception e) {
            log.warn("Failed to evaluate MVEL expression for strategy {}: {}", strategy, e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Evaluate an MVEL expression with the given context.
     * Uses caching for compiled expressions.
     */
    private Object evaluateExpression(String expression, Map<String, Object> context) {
        try {
            // Preprocess expression to handle YAML indentation issues
            String processedExpression = preprocessExpression(expression);
            
            // Try to get compiled expression from cache (using processed expression as key)
            Serializable compiledExpr = expressionCache.get(processedExpression);
            
            if (compiledExpr == null) {
                // Compile and cache the expression
                compiledExpr = MVEL.compileExpression(processedExpression);
                expressionCache.put(processedExpression, compiledExpr);
                log.debug("Compiled and cached MVEL expression: {}", processedExpression);
            }
            
            // Execute the compiled expression
            return MVEL.executeExpression(compiledExpr, context);
            
        } catch (Exception e) {
            log.warn("Error evaluating MVEL expression '{}': {}", expression, e.getMessage());
            throw e;
        }
    }
    
    /**
     * Preprocess MVEL expression to handle common issues:
     * 1. Trim each line to fix YAML indentation problems
     * 2. Remove empty lines
     * 3. Ensure proper block syntax
     */
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
    
    /**
     * Validate the expression result based on the partition strategy.
     */
    private Object validateResult(PartitionStrategy strategy, Object result, int totalPartitions) {
        switch (strategy) {
            case KEY:
                // For KEY strategy, any object can be used as a key
                // Kafka will hash it to determine partition
                return result;
                
            case EXPR:
                // For EXPR strategy, result must be an integer representing partition number
                if (result instanceof Number) {
                    int partition = ((Number) result).intValue();
                    
                    // Ensure partition is within valid range
                    if (partition < 0 || partition >= totalPartitions) {
                        log.warn("Partition number {} is out of range [0, {}). Using default partition 0.",
                                partition, totalPartitions);
                        return 0;
                    }
                    
                    return partition;
                } else {
                    log.warn("EXPR strategy requires integer result, got {}. Using default partition 0.",
                            result.getClass().getSimpleName());
                    return 0;
                }
                
            default:
                return null;
        }
    }
    
    /**
     * Clear the expression cache (useful for testing or dynamic expression updates).
     */
    public void clearCache() {
        expressionCache.clear();
        log.debug("Cleared MVEL expression cache");
    }
    
    /**
     * Get the current cache size (for monitoring).
     */
    public int getCacheSize() {
        return expressionCache.size();
    }
}