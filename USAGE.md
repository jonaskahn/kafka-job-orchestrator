# Quick Start Guide

## Installation

```bash
npm install @jonaskahn/kafka-job-orchestrator
# or
yarn add @jonaskahn/kafka-job-orchestrator
```

## Basic Usage

### Creating a Producer

```javascript
const { AbstractProducer } = require("@jonaskahn/kafka-job-orchestrator");

class MyProducer extends AbstractProducer {
  static get STATES() {
    return {
      PENDING: 1,
      COMPLETED: 2,
      FAILED: 3,
    };
  }

  constructor() {
    super({
      topic: "my-topic",
      redisKeyPrefix: "MY_APP",
    });
  }

  async getNextProcessingItems(criteria, limit, excludedIds) {
    // Fetch items from your database
    const items = await myDatabase
      .find({
        state: criteria.state,
        _id: { $nin: excludedIds },
      })
      .limit(limit);

    return items;
  }

  getItemId(item) {
    return item._id.toString();
  }

  getItemKey(item) {
    return item.name || item.id;
  }
}

// Usage
const producer = new MyProducer();
await producer.connect();
await producer.produceMessages(
  { state: MyProducer.STATES.PENDING },
  100,
  "new"
);
await producer.disconnect();
```

### Creating a Consumer

```javascript
const { AbstractConsumer } = require("@jonaskahn/kafka-job-orchestrator");

class MyConsumer extends AbstractConsumer {
  static get STATES() {
    return {
      PENDING: 1,
      COMPLETED: 2,
      FAILED: 3,
    };
  }

  constructor() {
    super({
      topic: "my-topic",
      consumerGroup: "my-consumer-group",
      redisKeyPrefix: "MY_APP",
    });
  }

  async getMessageId(messageData) {
    return messageData.id;
  }

  async getMessageKey(messageData) {
    return messageData.data.name || messageData.id;
  }

  async process(messageData) {
    try {
      // Your business logic here
      const result = await processItem(messageData.data);
      return { success: true, data: result };
    } catch (error) {
      return { success: false, error };
    }
  }

  async markItemAsCompleted(itemId) {
    await myDatabase.updateOne(
      { _id: itemId },
      { $set: { state: MyConsumer.STATES.COMPLETED } }
    );
  }

  async markItemAsFailed(itemId) {
    await myDatabase.updateOne(
      { _id: itemId },
      { $set: { state: MyConsumer.STATES.FAILED } }
    );
  }

  async isItemCompleted(itemId) {
    const item = await myDatabase.findOne({ _id: itemId });
    return item && item.state === MyConsumer.STATES.COMPLETED;
  }
}

// Usage
const consumer = new MyConsumer();
await consumer.connect();
await consumer.startConsuming();
```

## Environment Variables

```bash
# Required
MONGODB_URI=mongodb://localhost:27017/mydb
REDIS_PASSWORD=myredispassword
KAFKA_BROKERS=localhost:9092

# Optional
MAX_CONCURRENT_MESSAGES=3
KAFKA_TOPIC_PARTITIONS=4
KAFKA_REPLICATION_FACTOR=1
```

## Advanced Usage

### Using Services Directly

```javascript
const { services } = require("@jonaskahn/kafka-job-orchestrator");

// Use RedisService for custom deduplication
const redisService = new services.RedisService("MY_PREFIX");
await redisService.connect();
const isDuplicate = await redisService.isSentRecently("item-123", "hash-456");

// Use ConcurrentProcessor for custom concurrent tasks
const processor = new services.ConcurrentProcessor(5);
await processor.process(async () => {
  // Your async task
});
```

### Custom Topic Configuration

```javascript
const config = {
  topic: "my-topic",
  redisKeyPrefix: "MY_APP",
  topicOptions: {
    partitions: 4,
    replicationFactor: 2,
    configEntries: {
      "retention.ms": "604800000", // 7 days
      "compression.type": "gzip",
    },
  },
};
```

For more detailed documentation, see the [GitHub repository](https://github.com/jonaskahn/kafka-job-orchestrator).
