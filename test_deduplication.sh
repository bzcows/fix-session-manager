#!/bin/bash
# Deduplication Test Script for FIX Gateway
# Run on each node to test distributed deduplication

set -e

# Configuration
NODE_URL=${1:-"http://localhost:8080"}
TOPIC=${2:-"fix.SENDER.TARGET.output"}
SESSION_ID="FIX.4.4:SENDER->TARGET"
TIMESTAMP=$(date +%s)
MESSAGE_ID="test-dedup-$TIMESTAMP"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Testing Deduplication on $NODE_URL ===${NC}"
echo "Message ID: $MESSAGE_ID"
echo "Topic: $TOPIC"
echo "Session: $SESSION_ID"
echo ""

# Function to get current metrics
get_metrics() {
    echo -e "${YELLOW}Current metrics:${NC}"
    curl -s "$NODE_URL/actuator/metrics/fix.deduplication.messages.processed" | jq -r '.measurements[0].value // 0'
    curl -s "$NODE_URL/actuator/metrics/fix.deduplication.duplicates.detected" | jq -r '.measurements[0].value // 0'
}

# Function to check logs for duplicate message
check_logs() {
    echo -e "${YELLOW}Checking logs for duplicate detection...${NC}"
    # This assumes you have access to log files
    # Adjust based on your logging setup
    if [ -f "logs/application.log" ]; then
        grep -i "duplicate" logs/application.log | tail -5
    fi
}

# Create test message
create_test_message() {
    cat << EOF
{
  "messageId": "$MESSAGE_ID",
  "sessionId": "$SESSION_ID",
  "senderCompId": "SENDER",
  "targetCompId": "TARGET",
  "msgType": "D",
  "clOrdID": "ORD-$TIMESTAMP",
  "createdTimestamp": "$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")",
  "rawMessage": "8=FIX.4.4|9=145|35=D|49=SENDER|56=TARGET|34=1|52=$(date -u +"%Y%m%d-%H:%M:%S")|11=$MESSAGE_ID|55=AAPL|54=1|38=100|40=2|44=150.25|10=000|",
  "messageFingerprint": ""
}
EOF
}

# Get baseline metrics
echo -e "${YELLOW}=== Baseline Metrics ===${NC}"
BASELINE_PROCESSED=$(curl -s "$NODE_URL/actuator/metrics/fix.deduplication.messages.processed" | jq -r '.measurements[0].value // 0')
BASELINE_DUPLICATES=$(curl -s "$NODE_URL/actuator/metrics/fix.deduplication.duplicates.detected" | jq -r '.measurements[0].value // 0')
echo "Messages processed: $BASELINE_PROCESSED"
echo "Duplicates detected: $BASELINE_DUPLICATES"
echo ""

# Test 1: Send first message (should be processed)
echo -e "${YELLOW}=== Test 1: Sending First Message (should process) ===${NC}"
TEST_MSG=$(create_test_message)
echo "Sending message to topic $TOPIC..."
# Using rpk for Redpanda (adjust if using different producer)
if command -v rpk &> /dev/null; then
    echo "$TEST_MSG" | rpk topic produce "$TOPIC"
    echo "Message sent via rpk"
else
    echo -e "${RED}rpk not found. Using curl to simulate...${NC}"
    # Simulate by calling direct endpoint if available
    curl -X POST "$NODE_URL/api/test/message" \
        -H "Content-Type: application/json" \
        -d "$TEST_MSG" || true
fi

echo "Waiting 3 seconds for processing..."
sleep 3

# Test 2: Send duplicate message (should be detected as duplicate)
echo -e "${YELLOW}=== Test 2: Sending Duplicate Message (should be detected) ===${NC}"
echo "Sending EXACT SAME message again..."
if command -v rpk &> /dev/null; then
    echo "$TEST_MSG" | rpk topic produce "$TOPIC"
else
    curl -X POST "$NODE_URL/api/test/message" \
        -H "Content-Type: application/json" \
        -d "$TEST_MSG" || true
fi

echo "Waiting 3 seconds for processing..."
sleep 3

# Get final metrics
echo -e "${YELLOW}=== Final Metrics ===${NC}"
FINAL_PROCESSED=$(curl -s "$NODE_URL/actuator/metrics/fix.deduplication.messages.processed" | jq -r '.measurements[0].value // 0')
FINAL_DUPLICATES=$(curl -s "$NODE_URL/actuator/metrics/fix.deduplication.duplicates.detected" | jq -r '.measurements[0].value // 0')

echo "Messages processed: $FINAL_PROCESSED (baseline: $BASELINE_PROCESSED)"
echo "Duplicates detected: $FINAL_DUPLICATES (baseline: $BASELINE_DUPLICATES)"

# Calculate changes
PROCESSED_DIFF=$((FINAL_PROCESSED - BASELINE_PROCESSED))
DUPLICATES_DIFF=$((FINAL_DUPLICATES - BASELINE_DUPLICATES))

echo ""
echo -e "${YELLOW}=== Test Results ===${NC}"

if [ "$PROCESSED_DIFF" -eq 2 ] && [ "$DUPLICATES_DIFF" -eq 1 ]; then
    echo -e "${GREEN}✅ SUCCESS: Deduplication working correctly!${NC}"
    echo "- 2 messages processed"
    echo "- 1 duplicate detected"
    echo "- 1 unique message processed"
elif [ "$PROCESSED_DIFF" -eq 1 ] && [ "$DUPLICATES_DIFF" -eq 1 ]; then
    echo -e "${GREEN}✅ SUCCESS: Deduplication working (first message may have been processed before baseline)${NC}"
    echo "- 1 duplicate detected"
elif [ "$DUPLICATES_DIFF" -ge 1 ]; then
    echo -e "${GREEN}✅ PARTIAL SUCCESS: Duplicate detected${NC}"
    echo "- $DUPLICATES_DIFF duplicate(s) detected"
else
    echo -e "${RED}❌ FAILURE: No duplicate detected${NC}"
    echo "- Processed diff: $PROCESSED_DIFF"
    echo "- Duplicates diff: $DUPLICATES_DIFF"
    echo ""
    echo -e "${YELLOW}Troubleshooting:${NC}"
    echo "1. Check if gateway is running and consuming from topic: $TOPIC"
    echo "2. Verify Hazelcast cluster formation (all 3 nodes connected)"
    echo "3. Check application logs for errors"
    echo "4. Ensure message fingerprint generation is working"
fi

echo ""
echo -e "${YELLOW}=== Cross-Node Test Instructions ===${NC}"
echo "To test across 3 nodes:"
echo "1. Run this script on Node 1: ./test_deduplication.sh http://node1:8080"
echo "2. Run on Node 2 with SAME message ID: MESSAGE_ID=\"$MESSAGE_ID\" ./test_deduplication.sh http://node2:8080"
echo "3. Node 2 should detect duplicate via Hazelcast cluster"
echo ""
echo -e "${YELLOW}=== Quick Health Check ===${NC}"
echo "Cache size:"
curl -s "$NODE_URL/actuator/metrics/fix.deduplication.cache.size" | jq -r '.measurements[0].value // "N/A"'
echo "Duplicate rate:"
curl -s "$NODE_URL/actuator/metrics/fix.deduplication.duplicate.rate" | jq -r '.measurements[0].value // "N/A"'