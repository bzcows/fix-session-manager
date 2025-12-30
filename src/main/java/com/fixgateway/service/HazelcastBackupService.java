package com.fixgateway.service;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * OSS Hazelcast backup service - periodically saves map data to disk
 */
@Slf4j
@Service
@EnableScheduling
@RequiredArgsConstructor
public class HazelcastBackupService {

    private final HazelcastInstance hazelcastInstance;
    
    @Value("${hazelcast.backup.dir:./data/hazelcast}")
    private String backupDir;
    
    @Value("${hazelcast.backup.enabled:true}")
    private boolean backupEnabled;

    @PostConstruct
    public void initialize() {
        if (!backupEnabled) {
            log.info("Hazelcast backup is disabled");
            return;
        }
        
        try {
            Path backupPath = Paths.get(backupDir);
            Files.createDirectories(backupPath);
            log.info("Hazelcast backup directory: {}", backupPath.toAbsolutePath());
            
            // Load existing data on startup
            loadFromDisk();
        } catch (IOException e) {
            log.error("Failed to initialize backup directory", e);
        }
    }

    @Scheduled(fixedDelayString = "${hazelcast.backup.interval:60000}") // Default: 1 minute
    public void backupToDisk() {
        if (!backupEnabled) {
            return;
        }
        
        try {
            saveToDisk("fix-sequences");
            saveToDisk("fix-message-store");
            saveToDisk("fix-session-store");
            log.debug("Hazelcast data backed up to disk");
        } catch (Exception e) {
            log.error("Failed to backup Hazelcast data", e);
        }
    }

    private void saveToDisk(String mapName) throws IOException {
        IMap<String, ?> map = hazelcastInstance.getMap(mapName);
        Map<String, Object> snapshot = new HashMap<>(map);
        
        Path filePath = Paths.get(backupDir, mapName + ".dat");
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new FileOutputStream(filePath.toFile()))) {
            oos.writeObject(snapshot);
        }
    }

    @SuppressWarnings("unchecked")
    private void loadFromDisk() {
        loadMap("fix-sequences");
        loadMap("fix-message-store");
        loadMap("fix-session-store");
        log.info("Hazelcast data loaded from disk");
    }

    @SuppressWarnings("unchecked")
    private void loadMap(String mapName) {
        Path filePath = Paths.get(backupDir, mapName + ".dat");
        if (!Files.exists(filePath)) {
            log.debug("No backup file found for map: {}", mapName);
            return;
        }

        try (ObjectInputStream ois = new ObjectInputStream(
                new FileInputStream(filePath.toFile()))) {
            Map<String, Object> snapshot = (Map<String, Object>) ois.readObject();
            IMap<String, Object> map = hazelcastInstance.getMap(mapName);
            map.putAll(snapshot);
            log.info("Loaded {} entries from backup for map: {}", snapshot.size(), mapName);
        } catch (Exception e) {
            log.error("Failed to load backup for map: {}", mapName, e);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (backupEnabled) {
            log.info("Performing final backup before shutdown...");
            backupToDisk();
        }
    }
}
