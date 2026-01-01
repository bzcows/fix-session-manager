package com.fixgateway.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HazelcastConfig {

    @Value("${hazelcast.cluster.name:fix-session-cluster}")
    private String clusterName;

    @Bean
    public HazelcastInstance hazelcastInstance() {
        Config config = new Config();
        config.setClusterName(clusterName);
        
        // ---- HA (OSS-safe) ----
        // NOTE: CP Subsystem is an Enterprise feature and is intentionally NOT used.
        // Session ownership is coordinated using IMap + TTL-based leases.
        
        // OPEN SOURCE ONLY - No enterprise features
        // Using in-memory storage with backup for HA
        
        // Message Store Map Configuration
        MapConfig messageStoreConfig = new MapConfig()
                .setName("fix-message-store")
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setTimeToLiveSeconds(0)
                .setMaxIdleSeconds(0);
        config.addMapConfig(messageStoreConfig);

        // Session Store Map Configuration
        MapConfig sessionStoreConfig = new MapConfig()
                .setName("fix-session-store")
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setTimeToLiveSeconds(0)
                .setMaxIdleSeconds(0);
        config.addMapConfig(sessionStoreConfig);

        // Sequence Numbers Map Configuration
        MapConfig sequenceConfig = new MapConfig()
                .setName("fix-sequences")
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setTimeToLiveSeconds(0)
                .setMaxIdleSeconds(0);
        config.addMapConfig(sequenceConfig);

        // Ownership map for FIX session leadership (OSS-safe, TTL-based leases)
        MapConfig ownershipConfig = new MapConfig()
                .setName("fix-session-ownership")
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setTimeToLiveSeconds(15);
        config.addMapConfig(ownershipConfig);

        // Session assignment map for deterministic Active/Active sharding
        MapConfig assignmentConfig = new MapConfig()
                .setName("fix-session-assignments")
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setTimeToLiveSeconds(0) // never expire
                .setMaxIdleSeconds(0);
        config.addMapConfig(assignmentConfig);

        // Network configuration for single node (OSS)
        config.getNetworkConfig()
                .setPort(5701)
                .setPortAutoIncrement(true);
        
        config.getNetworkConfig().getJoin()
                .getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin()
                .getTcpIpConfig().setEnabled(false);

        return Hazelcast.newHazelcastInstance(config);
    }
}
