package com.fixgateway.model;

/**
 * Defines the strategy for partitioning messages in Kafka topics.
 */
public enum PartitionStrategy {
    /**
     * No partition key is set - uses Kafka's default partition assignment
     */
    NONE,
    
    /**
     * Expression returns a key that will be hashed to determine partition
     * The MVEL expression should return a String or Object that will be used as the partition key
     */
    KEY,
    
    /**
     * Expression returns an explicit partition number (0 to N-1)
     * The MVEL expression should return an Integer representing the target partition
     */
    EXPR
}