-- Create fixgateway database if it doesn't exist
SELECT 'CREATE DATABASE fixgateway'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'fixgateway')\gexec

-- Connect to fixgateway database
\c fixgateway

-- Create fix_messages table
CREATE TABLE IF NOT EXISTS fix_messages (
    id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(100) NOT NULL,
    sender_comp_id VARCHAR(50) NOT NULL,
    target_comp_id VARCHAR(50) NOT NULL,
    msg_type VARCHAR(10) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    message_category VARCHAR(20) NOT NULL,
    raw_message TEXT NOT NULL,
    parsed_fields TEXT,
    created_timestamp TIMESTAMPTZ NOT NULL,
    logged_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    node_id VARCHAR(100),
    sequence_number BIGINT,
    processing_duration_ms INTEGER
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_fix_messages_timestamp ON fix_messages(created_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fix_messages_session_ts ON fix_messages(session_id, created_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fix_messages_direction ON fix_messages(direction);
CREATE INDEX IF NOT EXISTS idx_fix_messages_msg_type ON fix_messages(msg_type) WHERE msg_type != '0';

-- Add comments for documentation
COMMENT ON TABLE fix_messages IS 'Stores all FIX messages processed by the gateway (inbound/outbound, application/admin)';
COMMENT ON COLUMN fix_messages.session_id IS 'FIX session identifier (e.g., FIX.4.4:SENDER->TARGET)';
COMMENT ON COLUMN fix_messages.sender_comp_id IS 'SenderCompID (tag 49)';
COMMENT ON COLUMN fix_messages.target_comp_id IS 'TargetCompID (tag 56)';
COMMENT ON COLUMN fix_messages.msg_type IS 'MsgType (tag 35)';
COMMENT ON COLUMN fix_messages.direction IS 'Message direction: INBOUND or OUTBOUND';
COMMENT ON COLUMN fix_messages.message_category IS 'Message category: APPLICATION or ADMIN';
COMMENT ON COLUMN fix_messages.raw_message IS 'Raw FIX message as string';
COMMENT ON COLUMN fix_messages.parsed_fields IS 'Parsed FIX fields as text (optional, could be JSON formatted)';
COMMENT ON COLUMN fix_messages.created_timestamp IS 'Timestamp when the FIX message was created/received';
COMMENT ON COLUMN fix_messages.logged_timestamp IS 'Timestamp when the message was logged to database';
COMMENT ON COLUMN fix_messages.node_id IS 'Node identifier that processed the message (for clustered deployments)';
COMMENT ON COLUMN fix_messages.sequence_number IS 'MsgSeqNum (tag 34) if available';
COMMENT ON COLUMN fix_messages.processing_duration_ms IS 'Processing duration in milliseconds (optional)';