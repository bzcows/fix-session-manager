package com.fixgateway.config;

import com.fixgateway.model.FixSessionConfig;
import com.fixgateway.model.FixSessionsProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaTopicInitializer {

    private final FixSessionsProperties sessionsProperties;

    @Value("${kafka.bootstrap.servers:localhost:9092}")
    private String kafkaBootstrapServers;

    @EventListener(ApplicationReadyEvent.class)
    public void createTopics() {
        log.info("Initializing Kafka topics for FIX sessions...");

        Properties config = new Properties();
        config.put("bootstrap.servers", kafkaBootstrapServers);
        config.put("client.id", "fix-gateway-topic-initializer");

        List<NewTopic> topics = new ArrayList<>();

        for (FixSessionConfig sessionConfig : sessionsProperties.getSessions()) {
            if (!sessionConfig.isEnabled()) {
                continue;
            }

            String inputTopic = String.format("fix.%s.%s.input",
                sessionConfig.getSenderCompId(), sessionConfig.getTargetCompId());
            String outputTopic = String.format("fix.%s.%s.output",
                sessionConfig.getSenderCompId(), sessionConfig.getTargetCompId());

            // Use configurable partition count for input topic (output topic remains 1 partition)
            int inputPartitions = sessionConfig.getInputPartitions();
            topics.add(new NewTopic(inputTopic, inputPartitions, (short) 1));
            topics.add(new NewTopic(outputTopic, 1, (short) 1));

            log.info("Preparing to create topics: {} ({} partitions) and {} (1 partition)",
                    inputTopic, inputPartitions, outputTopic);
        }

        // Also create DLQ topic
        topics.add(new NewTopic("fix.dlq", 1, (short) 1));
        log.info("Preparing to create DLQ topic: fix.dlq");

        try (AdminClient adminClient = AdminClient.create(config)) {
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
            log.info("Successfully created {} Kafka topics", topics.size());
        } catch (Exception e) {
            // Topics might already exist, which is fine
            if (e.getMessage() != null && e.getMessage().contains("TopicExistsException")) {
                log.info("Topics already exist (this is normal)");
            } else {
                log.warn("Error creating Kafka topics (they may already exist): {}", e.getMessage());
            }
        }
    }
}
