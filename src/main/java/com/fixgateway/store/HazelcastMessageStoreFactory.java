package com.fixgateway.store;

import com.hazelcast.core.HazelcastInstance;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import quickfix.MessageStore;
import quickfix.MessageStoreFactory;
import quickfix.SessionID;
import quickfix.SessionSettings;

@Component
@RequiredArgsConstructor
public class HazelcastMessageStoreFactory implements MessageStoreFactory {
    
    private final HazelcastInstance hazelcastInstance;

    @Override
    public MessageStore create(SessionID sessionID) {
        return new HazelcastMessageStore(sessionID, hazelcastInstance);
    }
}
