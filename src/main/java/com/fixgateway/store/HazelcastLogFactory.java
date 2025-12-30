package com.fixgateway.store;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import quickfix.*;

@Component
@RequiredArgsConstructor
public class HazelcastLogFactory implements LogFactory {
    
    private final HazelcastInstance hazelcastInstance;

    @Override
    public Log create(SessionID sessionID) {
        return new HazelcastLog(sessionID, hazelcastInstance);
    }

    private static class HazelcastLog implements Log {
        private final SessionID sessionID;
        private final IMap<String, String> logMap;
        private final String sessionPrefix;

        public HazelcastLog(SessionID sessionID, HazelcastInstance hazelcast) {
            this.sessionID = sessionID;
            this.sessionPrefix = sessionID.toString() + ":log:";
            this.logMap = hazelcast.getMap("fix-session-store");
        }

        @Override
        public void clear() {
            logMap.keySet().stream()
                .filter(key -> key.startsWith(sessionPrefix))
                .forEach(logMap::remove);
        }

        @Override
        public void onIncoming(String message) {
            logMap.put(sessionPrefix + System.currentTimeMillis() + ":in", message);
        }

        @Override
        public void onOutgoing(String message) {
            logMap.put(sessionPrefix + System.currentTimeMillis() + ":out", message);
        }

        @Override
        public void onEvent(String text) {
            logMap.put(sessionPrefix + System.currentTimeMillis() + ":event", text);
        }

        @Override
        public void onErrorEvent(String text) {
            logMap.put(sessionPrefix + System.currentTimeMillis() + ":error", text);
        }
    }
}
