# FIX Session Manager

A robust FIX protocol gateway with Kafka integration, built with Spring Boot, Apache Camel, QuickFIX/J, and Hazelcast.

## Features

- **Multi-Session Support**: Manage multiple FIX acceptor and initiator sessions
- **Kafka Integration**: Bidirectional message flow between FIX and Kafka topics
- **Content-Based Routing**: Route messages to specific Kafka partitions based on MVEL expressions
- **Message Filtering**: Automatically filters administrative messages (heartbeats, logon/logout, etc.)
- **State Persistence**: Hazelcast OSS in-memory storage with periodic disk backups
- **Message Envelope**: Structured message format with metadata
- **Error Handling**: Dead letter queue for failed message processing
- **Session Recovery**: Automatic session restoration after restart from disk backups

## Architecture

```
Kafka Output Topic → Camel → FIX Session
FIX Session → Camel → Kafka Input Topic
                ↓
    Hazelcast In-Memory Store (OSS)
                ↓
    Periodic Disk Backup (every 60s)
```

**Note**: Uses Hazelcast Open Source Edition with a custom backup service that periodically saves state to disk. No enterprise features required.

## Topic Naming Convention

- **Input (FIX → Kafka)**: `fix.{senderCompId}.{targetCompId}.input`
- **Output (Kafka → FIX)**: `fix.{senderCompId}.{targetCompId}.output`
- **Dead Letter**: `fix.dlq`

## Content-Based Routing

The system supports content-based routing to specific Kafka partitions using MVEL expressions. Three partition strategies are available:

### Partition Strategies

1. **NONE** (default): No partition key is set, uses Kafka's default partition assignment
2. **KEY**: Expression returns a key that will be hashed to determine partition
3. **EXPR**: Expression returns an explicit partition number (0 to N-1)

### Configuration Example

```yaml
fix:
  sessions:
    - sessionId: "FIX.4.4:GTWY->BANZ"
      type: "ACCEPTOR"
      senderCompId: "GTWY"
      targetCompId: "BANZ"
      # ... existing properties ...
      inputPartitions: 3  # Create topic with 3 partitions
      partitionStrategy: EXPR  # NONE, KEY, or EXPR
      partitionExpression: |
        // Example: Route based on MsgType and Symbol
        if (MsgType == "D" && ClOrdID == "1" && Symbol == "MSFT") {
          1  // Partition 1 for specific orders
        } else {
          2  // Partition 2 for all other messages
        }
```

### Available FIX Fields for MVEL Expressions

The following FIX message fields are available in the MVEL expression context:
- `MsgType`, `ClOrdID`, `Symbol`, `Side`, `OrderQty`, `Price`, `OrdType`, `TimeInForce`, `HandlInst`
- `SenderCompID`, `TargetCompID`, `MsgSeqNum`, `SendingTime`
- `rawMessage` (the complete raw FIX message string)
- `totalPartitions` (total number of partitions for the topic)

### Expression Examples

```java
// KEY strategy: Use ClOrdID as partition key
clOrdID

// EXPR strategy: Simple type-based routing
switch(MsgType) {
    case "D": return 0;
    case "8": return 1;
    default: return 2;
}

// EXPR strategy: Complex conditional routing
if (MsgType == "D" && Symbol == "MSFT") {
    0
} else if (MsgType == "D" && Symbol == "AAPL") {
    1
} else {
    2
}

// KEY strategy: Composite key for better distribution
Symbol + "-" + Side
```

## Message Envelope Format

```json
{
  "sessionId": "FIX.4.4:GTWY->BANZ",
  "senderCompId": "GTWY",
  "targetCompId": "BANZ",
  "msgType": "D",
  "createdTimestamp": "2025-01-15T10:30:00Z",
  "rawMessage": "8=FIX.4.4|9=178|35=D|..."
}
```

## Prerequisites

- JDK 21
- Maven 3.8+
- Kafka/RedPanda running on localhost:9092

## Build

```bash
cd fix-session-manager
mvn clean package
```

## Configuration

Edit `config/fix-sessions.yaml` to configure your FIX sessions.

## Run

```bash
java -jar target/fix-session-manager-1.0.0-SNAPSHOT.jar
```

## Testing with Kafka

### Produce to Output Topic (to send via FIX)
```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic fix.GTWY.BANZ.output

# Send JSON envelope
{"sessionId":"FIX.4.4:GTWY->BANZ","senderCompId":"GTWY","targetCompId":"BANZ","msgType":"D","createdTimestamp":"2025-01-15T10:30:00Z","rawMessage":"8=FIX.4.4|9=178|35=D|..."}
```

### Consume from Input Topic (received from FIX)
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic fix.GTWY.BANZ.input --from-beginning
```

## Directory Structure

```
fix-session-manager/
├── config/
│   └── fix-sessions.yaml      # FIX session configurations
├── data/
│   ├── hazelcast/             # Hazelcast persistence
│   └── quickfixj/             # QuickFIX/J files
├── logs/                      # Application logs
└── src/
    └── main/
        ├── java/
        │   └── com/fixgateway/
        │       ├── config/
        │       ├── model/
        │       ├── service/
        │       ├── component/
        │       ├── store/
        │       └── route/
        └── resources/
            └── application.yml
```

## Monitoring

- Health: http://localhost:8080/actuator/health
- Metrics: http://localhost:8080/actuator/metrics

## Tech Stack

- **JDK**: 21
- **Spring Boot**: 3.5.6
- **Apache Camel**: 4.14.0
- **QuickFIX/J**: 2.3.1
- **Hazelcast**: 5.5.0 (Open Source Edition ONLY - no enterprise features)
- **Kafka**: Compatible with RedPanda
- **MVEL2**: 2.5.0.Final (for expression evaluation)

## License

Proprietary
