# Kafka Job Orchestrator (KJO)

[![npm version](https://badge.fury.io/js/@jonaskahn%2Fkafka-job-orchestrator.svg)](https://badge.fury.io/js/@jonaskahn%2Fkafka-job-orchestrator)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![TypeScript](https://img.shields.io/badge/TypeScript-Ready-blue.svg)](src/index.d.ts)

A **database-agnostic**, high-availability distributed job orchestrator built on Kafka and Redis. Design resilient
cronjobs and task processors with built-in deduplication, concurrent processing, and clean architecture.

## 🚀 Key Features

- **🗄️ Database Agnostic**: Use any database (MongoDB, PostgreSQL, MySQL, etc.)
- **⚡ High Performance**: Concurrent processing with customizable limits
- **🔄 Deduplication**: Redis-based message deduplication and state tracking
- **🎯 Clean Architecture**: Abstract base classes following clean code principles
- **📦 TypeScript Ready**: Full TypeScript definitions included
- **🛡️ Production Ready**: Error handling, monitoring, and graceful shutdown
- **🔧 Kafka Native**: Built on KafkaJS with automatic topic management
- **📊 Monitoring**: Built-in statistics and status reporting

## 📋 Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Creating Producers](#creating-producers)
- [Creating Consumers](#creating-consumers)
- [Configuration](#configuration)
- [Database Integration](#database-integration)
- [Monitoring & Stats](#monitoring--stats)
- [Error Handling](#error-handling)
- [Examples](#examples)
- [API Reference](#api-reference)

## 📦 Installation

```bash
npm install @jonaskahn/kafka-job-orchestrator
```

### Dependencies

**Required:**

- Kafka 2.0+
- Redis 4.0+
- Node.js 14+

**Optional:**

- Any database of your choice (MongoDB, PostgreSQL, MySQL, etc.)

## 🚀 Quick Start

### 1. Infrastructure Setup

Start Kafka and Redis (example with Docker):

```bash
# Clone the repository for docker-compose example
git clone https://github.com/jonaskahn/kafka-job-orchestrator.git
cd kafka-job-orchestrator
docker-compose up -d kafka redis
```

### 2. Environment Configuration

Create `.env` file:

```bash
# Required - Kafka & Redis
KJO_KAFKA_BROKERS=localhost:9092
KJO_REDIS_URL=redis://localhost:6379
KJO_REDIS_PASSWORD=myredispassword

# Optional - Processing
KJO_MAX_CONCURRENT_MESSAGES=3
KJO_STATUS_REPORT_INTERVAL=30000

# Optional - Your Database (example with MongoDB)
DATABASE_URI=mongodb://localhost:27017/myapp
```

### 3. Basic Producer

```javascript
const { AbstractProducer } = require("@jonaskahn/kafka-job-orchestrator");

class TaskProducer extends AbstractProducer {
  constructor() {
    super({
      topic: "tasks",
      redisKeyPrefix: "TASKS:",
    });
  }

  // Implement required methods
  async getNextProcessingItems(criteria, limit, excludedIds) {
    // Fetch from your database
    return await this.db.tasks
      .find({
        status: "pending",
        _id: { $nin: excludedIds },
      })
      .limit(limit);
  }

  getItemId(item) {
    return item._id.toString();
  }

  getItemKey(item) {
    return item.name;
  }
}
```

### 4. Basic Consumer

```javascript
const { AbstractConsumer } = require("@jonaskahn/kafka-job-orchestrator");

class TaskConsumer extends AbstractConsumer {
  constructor() {
    super({
      topic: "tasks",
      consumerGroup: "task-processors",
      redisKeyPrefix: "TASKS:",
    });
  }

  // Implement required methods
  async getMessageId(messageData) {
    return messageData.id;
  }

  async getMessageKey(messageData) {
    return messageData.data.name;
  }

  async process(messageData) {
    // Your business logic here
    const result = await this.processTask(messageData.data);
    return result;
  }

  async markItemAsCompleted(itemId) {
    await this.db.tasks.updateOne({ _id: itemId }, { status: "completed" });
  }

  async markItemAsFailed(itemId) {
    await this.db.tasks.updateOne({ _id: itemId }, { status: "failed" });
  }

  async isItemCompleted(itemId) {
    const task = await this.db.getTask(itemId);
    return task?.status === "completed";
  }
}
```

### 5. Run Your System

```javascript
async function main() {
  const producer = new TaskProducer();
  const consumer = new TaskConsumer();

  await producer.connect();
  await consumer.connect();

  // Produce messages
  await producer.produceMessages({ status: "pending" }, 100, "batch-1");

  // Start consuming
  await consumer.startConsuming();
}

main().catch(console.error);
```

## 🏗 Core Concepts

### Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Your App      │    │     KJO      │    │  Infrastructure │
│                 │    │              │    │                 │
│ ┌─────────────┐ │    │ ┌──────────┐ │    │ ┌─────────────┐ │
│ │  Producer   ├─┼────┼─┤  Kafka   │ │    │ │   Kafka     │ │
│ │  Consumer   │ │    │ │  Redis   │ │    │ │   Redis     │ │
│ │             │ │    │ │ Services │ │    │ │   Database  │ │
│ └─────────────┘ │    │ └──────────┘ │    │ └─────────────┘ │
└─────────────────┘    └──────────────┘    └─────────────────┘
```

### State Management

KJO uses a **hybrid state model**:

- **Database States**: Persistent states (`pending`, `completed`, `failed`)
- **Redis States**: Virtual processing state (automatic cleanup, TTL-based)

```
┌─────────┐    Kafka     ┌─────────────┐    Database   ┌───────────┐
│ Pending ├─────────────→│ Processing* ├──────────────→│ Completed │
│         │              │   (Redis)   │               │ or Failed │
└─────────┘              └─────────────┘               └───────────┘
```

\*Processing state is virtual and managed automatically by KJO.

## 🏭 Creating Producers

Extend `AbstractProducer` to create message producers:

```javascript
const { AbstractProducer } = require("@jonaskahn/kafka-job-orchestrator");

class MyProducer extends AbstractProducer {
  constructor() {
    super({
      topic: "my-tasks",
      redisKeyPrefix: "MY_APP:",
      topicOptions: {
        partitions: 4,
        replicationFactor: 1,
        configEntries: {
          "retention.ms": "604800000", // 7 days
          "compression.type": "gzip",
        },
      },
    });

    // Initialize your database connection
    this.db = new DatabaseService();
  }

  // Required: Fetch items from your data source
  async getNextProcessingItems(criteria, limit, excludedIds) {
    return await this.db.findTasks(
      {
        status: criteria.status,
        _id: { $nin: excludedIds },
      },
      limit
    );
  }

  // Required: Extract unique ID
  getItemId(item) {
    return item._id.toString();
  }

  // Required: Extract display key for logging
  getItemKey(item) {
    return item.name || item.id;
  }

  // Optional: Custom success handling
  async onSuccess(itemId) {
    console.log(`✅ Successfully sent item: ${itemId}`);
  }

  // Optional: Custom failure handling
  async onFailed(itemId, error) {
    console.error(`❌ Failed to send item ${itemId}:`, error);
  }

  // Convenience method
  async producePendingTasks(limit = 100) {
    await this.produceMessages({ status: "pending" }, limit, "pending-batch");
  }
}
```

## 🏭 Creating Consumers

Extend `AbstractConsumer` to create message consumers:

```javascript
const { AbstractConsumer } = require("@jonaskahn/kafka-job-orchestrator");

class MyConsumer extends AbstractConsumer {
  constructor() {
    super({
      topic: "my-tasks",
      consumerGroup: "task-processors",
      redisKeyPrefix: "MY_APP:",
      topicOptions: {
        partitions: 4,
        replicationFactor: 1,
      },
    });

    this.db = new DatabaseService();
    this.processor = new TaskProcessor();
  }

  // Required: Extract message ID
  async getMessageId(messageData) {
    return messageData.id;
  }

  // Required: Extract message key for logging
  async getMessageKey(messageData) {
    return messageData.data.name || messageData.id;
  }

  // Required: Main business logic
  async process(messageData) {
    const { data } = messageData;

    // Your custom processing logic
    const result = await this.processor.processTask(data);

    // Return result for logging/monitoring
    return result;
  }

  // Required: Mark item as completed in database
  async markItemAsCompleted(itemId) {
    await this.db.updateTaskStatus(itemId, "completed");
  }

  // Required: Mark item as failed in database
  async markItemAsFailed(itemId) {
    await this.db.updateTaskStatus(itemId, "failed");
  }

  // Required: Check if item is already completed
  async isItemCompleted(itemId) {
    const task = await this.db.getTask(itemId);
    return task?.status === "completed";
  }

  // Optional: Handle processing results
  async handleProcessingResult(itemId, result) {
    await this.db.saveProcessingResult(itemId, result);
  }

  // Optional: Custom success handling
  async onSuccess(itemId) {
    console.log(`✅ Successfully processed: ${itemId}`);
  }

  // Optional: Custom failure handling
  async onFailed(itemId, error) {
    console.error(`❌ Failed to process ${itemId}:`, error);
    await this.db.logError(itemId, error);
  }
}
```

## ⚙️ Configuration

### Environment Variables

#### 🔧 Kafka Configuration

| Variable                                 | Default                        | Required | Description                                   |
| ---------------------------------------- | ------------------------------ | -------- | --------------------------------------------- |
| `KJO_KAFKA_BROKERS`                      | `localhost:9092`               | ✅       | Comma-separated list of Kafka brokers         |
| `KJO_KAFKA_CLIENT_ID`                    | `KJO_KAFKA_CLIENT.{timestamp}` | ❌       | Unique client identifier                      |
| `KJO_KAFKA_IDEMPOTENT`                   | `false`                        | ❌       | Enable idempotent producer (requires acks=-1) |
| `KJO_KAFKA_MAX_IN_FLIGHT_REQUESTS`       | `5`                            | ❌       | Maximum unacknowledged requests               |
| `KJO_KAFKA_MESSAGE_ACKNOWLEDGMENT_LEVEL` | `-1`                           | ❌       | Acknowledgment level (0, 1, -1)               |
| `KJO_KAFKA_MESSAGE_TIMEOUT`              | `30000`                        | ❌       | Message send timeout (ms)                     |
| `KJO_KAFKA_MESSAGE_COMPRESSION`          | `null`                         | ❌       | Compression type (gzip, snappy, lz4)          |
| `KJO_KAFKA_INITIAL_RETRY_TIME_MS`        | `100`                          | ❌       | Initial retry delay (ms)                      |
| `KJO_KAFKA_RETRY_COUNT`                  | `8`                            | ❌       | Number of retry attempts                      |
| `KJO_KAFKA_TRANSACTION_TIMEOUT_MS`       | `60000`                        | ❌       | Transaction timeout (ms)                      |
| `KJO_KAFKA_METADATA_MAX_AGE_MS`          | `300000`                       | ❌       | Metadata cache duration (ms)                  |
| `KJO_KAFKA_ALLOW_AUTO_TOPIC_CREATION`    | `false`                        | ❌       | Allow automatic topic creation                |

#### 🗃️ Redis Configuration

| Variable                               | Default                  | Required | Description                            |
| -------------------------------------- | ------------------------ | -------- | -------------------------------------- |
| `KJO_REDIS_URL`                        | `redis://localhost:6379` | ✅       | Redis connection URL                   |
| `KJO_REDIS_PASSWORD`                   | `null`                   | ❌       | Redis authentication password          |
| `KJO_REDIS_MAX_RETRY_ATTEMPTS`         | `5`                      | ❌       | Maximum connection retry attempts      |
| `KJO_REDIS_DELAY_MS`                   | `1000`                   | ❌       | Base retry delay (ms)                  |
| `KJO_REDIS_MAX_DELAY_MS`               | `30000`                  | ❌       | Maximum retry delay (ms)               |
| `KJO_REDIS_KEY_PREFIX`                 | `SUPERNOVA`              | ❌       | Redis key prefix for all operations    |
| `KJO_REDIS_PROCESSINGTTL_SECONDS`      | `10`                     | ❌       | Processing key TTL in seconds          |
| `KJO_REDIS_SENT_TTL_SECONDS`           | `120`                    | ❌       | Sent message tracking TTL in seconds   |
| `KJO_REDIS_KEY_SUFFIXES_PROCESSING`    | `_PROCESSING:`           | ❌       | Suffix for processing keys             |
| `KJO_REDIS_KEY_SUFFIXES_SENT`          | `SENT:`                  | ❌       | Suffix for sent message keys           |
| `KJO_REDIS_ENTITY_REQUIRED_PROPERTIES` | `_id,URL,state`          | ❌       | Comma-separated required entity fields |

#### 🔄 Consumer Configuration

| Variable                                           | Default    | Required | Description                      |
| -------------------------------------------------- | ---------- | -------- | -------------------------------- |
| `KJO_CONSUMSER_SESSION_TIMEOUT_MS`                 | `30000`    | ❌       | Consumer session timeout         |
| `KJO_CONSUMSER_REBALANCE_TIMEOUT_MS`               | `60000`    | ❌       | Rebalance timeout                |
| `KJO_CONSUMSER_HEARTBEAT_INTERVAL_MS`              | `3000`     | ❌       | Heartbeat interval               |
| `KJO_CONSUMSER_METADATA_MAX_AGE_MS`                | `300000`   | ❌       | Metadata cache duration          |
| `KJO_CONSUMSER_METADATA_ALLOW_AUTO_TOPIC_CREATION` | `false`    | ❌       | Allow auto topic creation        |
| `KJO_CONSUMSER_MAX_BYTES_PER_PARTITION`            | `1048576`  | ❌       | Max bytes per partition (1MB)    |
| `KJO_CONSUMSER_MIN_BYTES`                          | `1`        | ❌       | Minimum fetch bytes              |
| `KJO_CONSUMSER_MAX_BYTES`                          | `10485760` | ❌       | Maximum fetch bytes (10MB)       |
| `KJO_CONSUMSER_MAX_WAIT_TIME_MS`                   | `5000`     | ❌       | Maximum fetch wait time          |
| `KJO_CONSUMSER_MAX_IN_FLIGHT_REQUESTS`             | `1`        | ❌       | Max concurrent fetch requests    |
| `KJO_CONSUMSER_MAX_CONCURRENT_MESSAGES`            | `3`        | ❌       | Concurrent message processing    |
| `KJO_CONSUMSER_STATUS_REPORT_INTERVAL_MS`          | `30000`    | ❌       | Status reporting interval        |
| `KJO_CONSUMSER_PARTITIONS_CONSUMED_CONCURRENTLY`   | `1`        | ❌       | Concurrent partition consumption |
| `KJO_CONSUMSER_CONNECTION_RETRY_ATTEMPTS`          | `5`        | ❌       | Connection retry attempts        |
| `KJO_CONSUMSER_CONNECTION_RETRY_DELAY`             | `2000`     | ❌       | Connection retry delay           |

#### ⚙️ Consumer Retry Configuration

| Variable                           | Default | Required | Description              |
| ---------------------------------- | ------- | -------- | ------------------------ |
| `KJO_CONSUMSER_RETRIES`            | `10`    | ❌       | Number of retry attempts |
| `KJO_CONSUMSER_RETRY_INITIAL_TIME` | `1000`  | ❌       | Initial retry time (ms)  |
| `KJO_CONSUMSER_RETRY_DELAY`        | `30000` | ❌       | Maximum retry delay (ms) |
| `KJO_CONSUMSER_RETRY_MULTIPLIER`   | `2`     | ❌       | Retry delay multiplier   |

#### 🚨 Error Recovery Configuration

| Variable                                      | Default | Required | Description                     |
| --------------------------------------------- | ------- | -------- | ------------------------------- |
| `KJO_CONSUMSER_ERROR_RECOVERY_DELAYS_DEFAULT` | `5000`  | ❌       | Default error recovery delay    |
| `KJO_CONSUMSER_ERROR_RECOVERY_NETWORK_ERROR`  | `10000` | ❌       | Network error recovery delay    |
| `KJO_CONSUMSER_ERROR_BUSINESS_ERROR`          | `2000`  | ❌       | Business error recovery delay   |
| `KJO_CONSUMSER_RATE_LIMIT_ERROR`              | `30000` | ❌       | Rate limit error recovery delay |

#### ⚡ Concurrent Processing Configuration

| Variable                                | Default | Required | Description                    |
| --------------------------------------- | ------- | -------- | ------------------------------ |
| `KJO_CONCURRENT_MAX_EXECUTOR`           | `5`     | ❌       | Maximum concurrent tasks       |
| `KJO_CONCURRENT_POLLING_INTERVAL_MS`    | `100`   | ❌       | Task polling interval (ms)     |
| `KJO_CONCURRENT_TASK_ID_LENGTH`         | `9`     | ❌       | Generated task ID length       |
| `KJO_CONCURRENT_SUCCESS_RATE_PRECISION` | `2`     | ❌       | Success rate decimal precision |

#### 📋 Topic Configuration

| Variable                             | Default     | Required | Description                     |
| ------------------------------------ | ----------- | -------- | ------------------------------- |
| `KJO_KAFKA_TOPIC_PARTITIONS`         | `4`         | ❌       | Default topic partitions        |
| `KJO_KAFKA_TOPIC_REPLICATION_FACTOR` | `1`         | ❌       | Default replication factor      |
| `KJO_KAFKA_TOPIC_AUTO_CREATE_TOPICS` | `false`     | ❌       | Enable automatic topic creation |
| `KJO_KAFKA_TOPIC_RETENTION_MS`       | `604800000` | ❌       | Topic retention time (7 days)   |
| `KJO_KAFKA_TOPIC_SEGMENT_MS`         | `86400000`  | ❌       | Topic segment time (1 day)      |
| `KJO_KAFKA_TOPIC_COMPRESSION_TYPE`   | `producer`  | ❌       | Topic compression type          |
| `KJO_KAFKA_TOPIC_CLEANUP_POLICY`     | `delete`    | ❌       | Topic cleanup policy            |

#### 📝 Logging Configuration

| Variable     | Default  | Required | Description                              |
| ------------ | -------- | -------- | ---------------------------------------- |
| `LOG_LEVEL`  | `info`   | ❌       | Logging level (error, warn, info, debug) |
| `LOG_FORMAT` | `simple` | ❌       | Log format (simple, json, detailed)      |

#### 🔧 Message Processing

| Variable                     | Default | Required | Description                                   |
| ---------------------------- | ------- | -------- | --------------------------------------------- |
| `KJO_DEDUPLICATION_STRATEGY` | `redis` | ❌       | Deduplication strategy (redis, kafka, hybrid) |
| `STATUS_REPORT_INTERVAL`     | `30000` | ❌       | Legacy status report interval (ms)            |

#### 📋 Quick Setup Example

```bash
# Minimum required configuration
KJO_KAFKA_BROKERS=localhost:9092
KJO_REDIS_URL=redis://localhost:6379

# Production recommended settings
KJO_KAFKA_BROKERS=kafka1:9092,kafka2:9092,kafka3:9092
KJO_REDIS_URL=redis://redis-cluster:6379
KJO_REDIS_PASSWORD=your-secure-password
KJO_REDIS_KEY_PREFIX=PRODUCTION
KJO_KAFKA_TOPIC_PARTITIONS=8
KJO_KAFKA_TOPIC_REPLICATION_FACTOR=3
KJO_CONSUMSER_MAX_CONCURRENT_MESSAGES=10
KJO_KAFKA_IDEMPOTENT=true
KJO_KAFKA_MESSAGE_ACKNOWLEDGMENT_LEVEL=-1
KJO_DEDUPLICATION_STRATEGY=hybrid
LOG_LEVEL=info
LOG_FORMAT=json
```

### Programmatic Configuration

```javascript
// Producer configuration
const producerConfig = {
  topic: "my-topic",
  redisKeyPrefix: "MY_APP:",
  topicOptions: {
    partitions: 4,
    replicationFactor: 2,
    autoCreate: true,
    configEntries: {
      "retention.ms": "604800000", // 7 days
      "segment.ms": "86400000", // 1 day
      "compression.type": "gzip",
      "max.message.bytes": "1048576", // 1MB
      "cleanup.policy": "delete",
    },
  },
};

// Consumer configuration
const consumerConfig = {
  topic: "my-topic",
  consumerGroup: "my-group",
  redisKeyPrefix: "MY_APP:",
  topicOptions: {
    /* same as producer */
  },
};
```

## 🗄️ Database Integration

KJO is **database-agnostic**. Here are integration examples:

### MongoDB

```javascript
const { MongoClient } = require("mongodb");

class MongoTaskService {
  async connect() {
    this.client = new MongoClient(process.env.DATABASE_URI);
    await this.client.connect();
    this.db = this.client.db();
  }

  async findTasks(criteria, limit) {
    return await this.db
      .collection("tasks")
      .find(criteria)
      .limit(limit)
      .toArray();
  }

  async updateTaskStatus(taskId, status) {
    await this.db
      .collection("tasks")
      .updateOne({ _id: taskId }, { $set: { status, updatedAt: new Date() } });
  }
}
```

### PostgreSQL

```javascript
const { Pool } = require("pg");

class PostgresTaskService {
  constructor() {
    this.pool = new Pool({ connectionString: process.env.DATABASE_URI });
  }

  async findTasks(status, limit) {
    const { rows } = await this.pool.query(
      "SELECT * FROM tasks WHERE status = $1 LIMIT $2",
      [status, limit]
    );
    return rows;
  }

  async updateTaskStatus(taskId, status) {
    await this.pool.query(
      "UPDATE tasks SET status = $1, updated_at = NOW() WHERE id = $2",
      [status, taskId]
    );
  }
}
```

### MySQL

```javascript
const mysql = require("mysql2/promise");

class MySQLTaskService {
  constructor() {
    this.pool = mysql.createPool(process.env.DATABASE_URI);
  }

  async findTasks(status, limit) {
    const [rows] = await this.pool.execute(
      "SELECT * FROM tasks WHERE status = ? LIMIT ?",
      [status, limit]
    );
    return rows;
  }

  async updateTaskStatus(taskId, status) {
    await this.pool.execute(
      "UPDATE tasks SET status = ?, updated_at = NOW() WHERE id = ?",
      [status, taskId]
    );
  }
}
```

## 📊 Monitoring & Stats

### Real-time Statistics

```javascript
// Get detailed processing status
const status = consumer.getConcurrentStatus();
console.log(status);
/*
{
  concurrency: { max: 5, active: 2, queued: 0, available: 3 },
  performance: { 
    processed: 150, 
    failed: 5, 
    successRate: "96.8%",
    maxConcurrentReached: 5 
  },
  queue: { length: 0, oldestWaitingMs: 0 }
}
*/

// Show formatted stats in console
consumer.showStats();

// Update concurrency at runtime
consumer.setMaxConcurrency(10);
```

### Built-in Services

```javascript
const {
  ConcurrentProcessor,
  MessageService,
  RedisService,
  TopicService,
} = require("@jonaskahn/kafka-job-orchestrator");

// Direct service usage for advanced scenarios
const concurrentProcessor = new ConcurrentProcessor(5);
const messageService = new MessageService();
const redisService = new RedisService({ keyPrefix: "MY_PREFIX" });
const topicService = new TopicService();
```

## 🛡️ Error Handling

### Automatic Recovery

- **Crash Recovery**: Virtual processing state prevents stuck items
- **Network Errors**: Automatic retry with exponential backoff
- **Rate Limiting**: Automatic partition pause and resume
- **Graceful Shutdown**: Waits for active tasks to complete

### Custom Error Handling

```javascript
class RobustConsumer extends AbstractConsumer {
  async process(messageData) {
    try {
      return await this.businessLogic(messageData);
    } catch (error) {
      if (error.code === "RATE_LIMIT") {
        // Custom rate limit handling
        await this.handleRateLimit(error);
        throw error; // Will trigger automatic retry
      }

      if (error.code === "VALIDATION_ERROR") {
        // Don't retry validation errors
        console.error("Validation failed:", error.message);
        return { success: false, error: error.message };
      }

      throw error; // Let KJO handle other errors
    }
  }

  async onFailed(itemId, error) {
    // Log to monitoring system
    await this.logger.error("Processing failed", { itemId, error });

    // Update failure count in database
    await this.db.incrementFailureCount(itemId);

    // Send alert if failure count exceeds threshold
    const item = await this.db.getTask(itemId);
    if (item.failureCount > 3) {
      await this.alerting.sendAlert(
        `Task ${itemId} failed ${item.failureCount} times`
      );
    }
  }
}
```

## 📚 Examples

Complete examples are available in the [examples directory](examples/):

### Web Crawler

```bash
# Run the crawler example
npm run example:crawler
```

### Email Processing

```bash
# Producer
npm run example:email:producer

# Consumer
npm run example:email:consumer
```

### Custom Implementation

```bash
# Generate sample data
npm run generate-data

# Run basic producer/consumer
npm run producer
npm run consumer
```

## 📖 API Reference

### Core Classes

#### `AbstractProducer`

```typescript
abstract class AbstractProducer {
  constructor(config: ProducerConfig);

  connect(): Promise<void>;

  disconnect(): Promise<void>;

  produceMessages(
    criteria: any,
    limit: number,
    messageType: string
  ): Promise<void>;

  // Abstract methods to implement
  abstract getNextProcessingItems(
    criteria: any,
    limit: number,
    excludedIds: string[]
  ): Promise<any[]>;

  abstract getItemId(item: any): string;

  abstract getItemKey(item: any): string;

  // Optional hooks
  onSuccess(itemId: string): Promise<void>;

  onFailed(itemId: string, error: Error): Promise<void>;
}
```

#### `AbstractConsumer`

```typescript
abstract class AbstractConsumer {
  constructor(config: ConsumerConfig);

  connect(): Promise<void>;

  disconnect(): Promise<void>;

  startConsuming(): Promise<void>;

  getConcurrentStatus(): DetailedConcurrentStatus;

  setMaxConcurrency(newMaxConcurrency: number): void;

  showStats(): void;

  // Abstract methods to implement
  abstract getMessageId(messageData: MessageData): Promise<string> | string;

  abstract getMessageKey(messageData: MessageData): Promise<string> | string;

  abstract process(messageData: MessageData): Promise<any>;

  abstract markItemAsCompleted(itemId: string): Promise<void>;

  abstract markItemAsFailed(itemId: string): Promise<void>;

  abstract isItemCompleted(itemId: string): Promise<boolean>;

  // Optional hooks
  handleProcessingResult(itemId: string, result: any): Promise<void>;

  onSuccess(itemId: string): Promise<void>;

  onFailed(itemId: string, error: Error): Promise<void>;
}
```

### Configuration Interfaces

```typescript
interface ProducerConfig {
  topic: string;
  redisKeyPrefix: string;
  topicOptions?: TopicOptions;
}

interface ConsumerConfig {
  topic: string;
  consumerGroup: string;
  redisKeyPrefix: string;
  topicOptions?: TopicOptions;
}

interface TopicOptions {
  partitions?: number;
  replicationFactor?: number;
  configEntries?: Record<string, string>;
  autoCreate?: boolean;
}
```

### TypeScript Support

Full TypeScript definitions are included:

```typescript
import {
  AbstractProducer,
  AbstractConsumer,
  ProducerConfig,
  ConsumerConfig,
  MessageData,
} from "@jonaskahn/kafka-job-orchestrator";

class TypedProducer extends AbstractProducer {
  constructor() {
    super({
      topic: "typed-tasks",
      redisKeyPrefix: "TYPED:",
    });
  }

  // TypeScript will enforce implementation of abstract methods
}
```

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Follow clean code principles (see [CLEAN_CODE.md](CLEAN_CODE.md))
4. Commit your changes (`git commit -m 'Add amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙋‍♂️ Support

- 📖 [Documentation](https://github.com/jonaskahn/kafka-job-orchestrator)
- 🐛 [Issue Tracker](https://github.com/jonaskahn/kafka-job-orchestrator/issues)
- 💬 [Discussions](https://github.com/jonaskahn/kafka-job-orchestrator/discussions)

---

**Made with ❤️ by [Jonas Kahn](https://github.com/jonaskahn)**
