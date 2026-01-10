package com.fixgateway.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import quickfix.Message;
import quickfix.field.*;
import quickfix.fix44.NewOrderSingle;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility to parse FIX messages and extract fields for expression evaluation.
 */
@Slf4j
@Component
public class FixMessageParser {

    /**
     * Parse a raw FIX message string and extract common fields for expression evaluation.
     * 
     * @param rawMessage The raw FIX message string
     * @return Map of field names to values (as Objects)
     */
    public Map<String, Object> parseFixMessage(String rawMessage) {
        Map<String, Object> context = new HashMap<>();
        
        if (rawMessage == null || rawMessage.trim().isEmpty()) {
            return context;
        }
        
        try {
            Message message = new Message(rawMessage);
            
            // Extract header fields
            try {
                context.put("MsgType", message.getHeader().getString(MsgType.FIELD));
            } catch (Exception e) {
                log.debug("MsgType field not found in message");
            }
            
            try {
                context.put("SenderCompID", message.getHeader().getString(SenderCompID.FIELD));
            } catch (Exception e) {
                log.debug("SenderCompID field not found in message");
            }
            
            try {
                context.put("TargetCompID", message.getHeader().getString(TargetCompID.FIELD));
            } catch (Exception e) {
                log.debug("TargetCompID field not found in message");
            }
            
            try {
                context.put("MsgSeqNum", message.getHeader().getInt(MsgSeqNum.FIELD));
            } catch (Exception e) {
                log.debug("MsgSeqNum field not found in message");
            }
            
            try {
                context.put("SendingTime", message.getHeader().getString(SendingTime.FIELD));
            } catch (Exception e) {
                log.debug("SendingTime field not found in message");
            }
            
            // Extract common body fields
            try {
                context.put("ClOrdID", message.getString(ClOrdID.FIELD));
            } catch (Exception e) {
                log.debug("ClOrdID field not found in message");
            }
            
            try {
                context.put("Symbol", message.getString(Symbol.FIELD));
            } catch (Exception e) {
                log.debug("Symbol field not found in message");
            }
            
            try {
                context.put("Side", message.getChar(Side.FIELD));
            } catch (Exception e) {
                log.debug("Side field not found in message");
            }
            
            try {
                context.put("OrderQty", message.getDecimal(OrderQty.FIELD));
            } catch (Exception e) {
                log.debug("OrderQty field not found in message");
            }
            
            try {
                context.put("Price", message.getDecimal(Price.FIELD));
            } catch (Exception e) {
                log.debug("Price field not found in message");
            }
            
            try {
                context.put("OrdType", message.getChar(OrdType.FIELD));
            } catch (Exception e) {
                log.debug("OrdType field not found in message");
            }
            
            try {
                context.put("TimeInForce", message.getChar(TimeInForce.FIELD));
            } catch (Exception e) {
                log.debug("TimeInForce field not found in message");
            }
            
            try {
                context.put("HandlInst", message.getChar(HandlInst.FIELD));
            } catch (Exception e) {
                log.debug("HandlInst field not found in message");
            }
            
            // Add raw message for advanced parsing if needed
            context.put("rawMessage", rawMessage);
            
        } catch (Exception e) {
            log.warn("Failed to parse FIX message for expression evaluation: {}", e.getMessage());
            // Return empty context rather than failing
        }
        
        return context;
    }
    
    /**
     * Parse a QuickFIX/J Message object and extract fields for expression evaluation.
     * 
     * @param message The QuickFIX/J Message object
     * @return Map of field names to values (as Objects)
     */
    public Map<String, Object> parseFixMessage(Message message) {
        Map<String, Object> context = new HashMap<>();
        
        if (message == null) {
            return context;
        }
        
        try {
            // Extract header fields
            try {
                context.put("MsgType", message.getHeader().getString(MsgType.FIELD));
            } catch (Exception e) {
                log.debug("MsgType field not found in message");
            }
            
            try {
                context.put("SenderCompID", message.getHeader().getString(SenderCompID.FIELD));
            } catch (Exception e) {
                log.debug("SenderCompID field not found in message");
            }
            
            try {
                context.put("TargetCompID", message.getHeader().getString(TargetCompID.FIELD));
            } catch (Exception e) {
                log.debug("TargetCompID field not found in message");
            }
            
            try {
                context.put("MsgSeqNum", message.getHeader().getInt(MsgSeqNum.FIELD));
            } catch (Exception e) {
                log.debug("MsgSeqNum field not found in message");
            }
            
            try {
                context.put("SendingTime", message.getHeader().getString(SendingTime.FIELD));
            } catch (Exception e) {
                log.debug("SendingTime field not found in message");
            }
            
            // Extract common body fields
            try {
                context.put("ClOrdID", message.getString(ClOrdID.FIELD));
            } catch (Exception e) {
                log.debug("ClOrdID field not found in message");
            }
            
            try {
                context.put("Symbol", message.getString(Symbol.FIELD));
            } catch (Exception e) {
                log.debug("Symbol field not found in message");
            }
            
            try {
                context.put("Side", message.getChar(Side.FIELD));
            } catch (Exception e) {
                log.debug("Side field not found in message");
            }
            
            try {
                context.put("OrderQty", message.getDecimal(OrderQty.FIELD));
            } catch (Exception e) {
                log.debug("OrderQty field not found in message");
            }
            
            try {
                context.put("Price", message.getDecimal(Price.FIELD));
            } catch (Exception e) {
                log.debug("Price field not found in message");
            }
            
            try {
                context.put("OrdType", message.getChar(OrdType.FIELD));
            } catch (Exception e) {
                log.debug("OrdType field not found in message");
            }
            
            try {
                context.put("TimeInForce", message.getChar(TimeInForce.FIELD));
            } catch (Exception e) {
                log.debug("TimeInForce field not found in message");
            }
            
            try {
                context.put("HandlInst", message.getChar(HandlInst.FIELD));
            } catch (Exception e) {
                log.debug("HandlInst field not found in message");
            }
            
            // Add raw message string
            context.put("rawMessage", message.toString());
            
        } catch (Exception e) {
            log.warn("Failed to parse FIX Message object for expression evaluation: {}", e.getMessage());
        }
        
        return context;
    }
}