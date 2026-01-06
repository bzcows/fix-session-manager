package com.fixgateway.store;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import quickfix.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;

public class HazelcastMessageStore implements MessageStore {
    
    private final SessionID sessionID;
    private final IMap<String, Integer> sequences;
    private final IMap<String, String> messages;
    private final IMap<String, String> metadata;
    private final String sessionPrefix;

    public HazelcastMessageStore(SessionID sessionID, HazelcastInstance hazelcast) {
        this.sessionID = sessionID;
        this.sessionPrefix = sessionID.toString() + ":";
        this.sequences = hazelcast.getMap("fix-sequences");
        this.messages = hazelcast.getMap("fix-message-store");
        this.metadata = hazelcast.getMap("fix-session-store");
    }

    @Override
    public boolean set(int sequence, String message) throws IOException {
        messages.put(sessionPrefix + sequence, message);
        return true;
    }

    @Override
    public void get(int startSequence, int endSequence, Collection<String> messages) throws IOException {
        for (int i = startSequence; i <= endSequence; i++) {
            String message = this.messages.get(sessionPrefix + i);
            if (message != null) {
                messages.add(message);
            }
        }
    }

    @Override
    public int getNextSenderMsgSeqNum() throws IOException {
        return sequences.getOrDefault(sessionPrefix + "sender", 1);
    }

    @Override
    public int getNextTargetMsgSeqNum() throws IOException {
        return sequences.getOrDefault(sessionPrefix + "target", 1);
    }

    @Override
    public void setNextSenderMsgSeqNum(int next) throws IOException {
        sequences.put(sessionPrefix + "sender", next);
    }

    @Override
    public void setNextTargetMsgSeqNum(int next) throws IOException {
        sequences.put(sessionPrefix + "target", next);
    }

    @Override
    public void incrNextSenderMsgSeqNum() throws IOException {
        // Use Hazelcast atomic operation for sequence increments to prevent race conditions
        sequences.compute(sessionPrefix + "sender", (key, current) -> {
            if (current == null) return 2;
            return current + 1;
        });
    }

    @Override
    public void incrNextTargetMsgSeqNum() throws IOException {
        sequences.compute(sessionPrefix + "target", (key, current) -> {
            if (current == null) return 2;
            return current + 1;
        });
    }

    @Override
    public Date getCreationTime() throws IOException {
        String timeStr = metadata.getOrDefault(sessionPrefix + "creationTime", 
            String.valueOf(System.currentTimeMillis()));
        return new Date(Long.parseLong(timeStr));
    }

    @Override
    public void reset() throws IOException {
        // Clear all messages for this session
        messages.keySet().stream()
            .filter(key -> key.startsWith(sessionPrefix))
            .forEach(messages::remove);
        
        // Reset sequences
        sequences.put(sessionPrefix + "sender", 1);
        sequences.put(sessionPrefix + "target", 1);
        metadata.put(sessionPrefix + "creationTime", String.valueOf(System.currentTimeMillis()));
    }

    @Override
    public void refresh() throws IOException {
        // No-op for Hazelcast as it's always current
    }
}
