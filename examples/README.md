# Implementation Examples

This directory contains example implementations of the abstract consumer and producer classes.

## Creating a New Consumer

To create a new consumer, extend the `AbstractConsumer` class and implement the required abstract methods:

```javascript
const AbstractConsumer = require("../src/abstracts/AbstractConsumer");

class MyConsumer extends AbstractConsumer {
    // Define your state constants
    // Note: PROCESSING state is virtual (Redis-only), not stored in database
    static get STATES() {
        return {
            PENDING: 1,
            COMPLETED: 2,
            FAILED: 3,
        };
    }

    constructor() {
        const config = {
            // Required fields
            topic: "my-topic",
            consumerGroup: "my-consumer-group",
            redisKeyPrefix: "MY_CONSUMER",

            // Optional: Topic configuration (creates topic if doesn't exist)
            topicOptions: {
                partitions: 4, // Default: process.env.KAFKA_TOPIC_PARTITIONS || 1
                replicationFactor: 2, // Default: process.env.KAFKA_REPLICATION_FACTOR || 1
                configEntries: {
                    // Additional Kafka topic configs
                    "retention.ms": "604800000", // 7 days
                    "compression.type": "gzip",
                    "max.message.bytes": "1048576", // 1MB
                },
            },
        };
        super(config);
    }

    // Implement required abstract methods

    async getMessageId(messageData) {
        return messageData.id;
    }

    async getMessageKey(messageData) {
        return messageData.name || messageData.id;
    }

    async process(messageData) {
        // Your business logic here
        console.log("Processing:", messageData);
        return {success: true};
    }

    // NOTE: markItemAsProcessing is no longer needed!
    // Processing state is now virtual (Redis-only) and handled automatically

    async markItemAsCompleted(itemId) {
        // Update your data source to mark item as completed
        await myDatabase.updateState(itemId, MyConsumer.STATES.COMPLETED);
    }

    async markItemAsFailed(itemId) {
        // Update your data source to mark item as failed
        await myDatabase.updateState(itemId, MyConsumer.STATES.FAILED);
    }

    async isItemCompleted(itemId) {
        // Check if item is already completed
        const item = await myDatabase.findById(itemId);
        return item && item.state === MyConsumer.STATES.COMPLETED;
    }

    // Optional: Override hook methods

    async handleProcessingResult(itemId, result) {
        // Save processing results
        await myDatabase.saveResult(itemId, result);
    }

    async onSuccess(itemId) {
        console.log(`Successfully processed: ${itemId}`);
    }

    async onFailed(itemId, error) {
        console.error(`Failed to process ${itemId}:`, error);
    }
}
```

## Creating a New Producer

To create a new producer, extend the `AbstractProducer` class and implement the required abstract methods:

```javascript
const AbstractProducer = require("../src/abstracts/AbstractProducer");

class MyProducer extends AbstractProducer {
    // Define your state constants
    static get STATES() {
        return {
            PENDING: 1,
            PROCESSING: 2,
            COMPLETED: 3,
            FAILED: 4,
        };
    }

    constructor() {
        const config = {
            // Required fields
            topic: "my-topic",
            redisKeyPrefix: "MY_PRODUCER",

            // Optional: Topic configuration (creates topic if doesn't exist)
            topicOptions: {
                partitions: 4, // Default: process.env.KAFKA_TOPIC_PARTITIONS || 1
                replicationFactor: 2, // Default: process.env.KAFKA_REPLICATION_FACTOR || 1
                configEntries: {
                    // Additional Kafka topic configs
                    "retention.ms": "604800000", // 7 days
                    "compression.type": "gzip",
                    "max.message.bytes": "1048576", // 1MB
                },
            },
        };
        super(config);
    }

    // Create convenience methods for different types of messages
    async produceNewItems(limit = 100) {
        const criteria = {state: MyProducer.STATES.PENDING};
        return await this.produceMessages(criteria, limit, "new");
    }

    async produceFailedItems(limit = 50) {
        const criteria = {state: MyProducer.STATES.FAILED};
        return await this.produceMessages(criteria, limit, "retry");
    }

    // Implement required abstract methods

    async getNextProcessingItems(criteria, limit, excludedIds) {
        // Fetch items from your data source based on criteria
        const {state, priority, category} = criteria;

        return await myDatabase.findItems({
            state: state,
            priority: priority,
            category: category,
            _id: {$notIn: excludedIds},
            limit: limit,
        });
    }

    getItemId(item) {
        return item._id || item.id;
    }

    getItemKey(item) {
        return item.name || item.title || item.id;
    }

    // Optional: Override hook methods

    async onSuccess(itemId) {
        console.log(`Successfully sent: ${itemId}`);
    }

    async onFailed(itemId, error) {
        console.error(`Failed to send ${itemId}:`, error);
    }
}
```

## Key Concepts

### State Management

Each implementation defines its own state constants. The abstract classes no longer enforce specific state values,
giving you flexibility to design your state machine.

#### Virtual Processing State

The framework now uses a **virtual processing state** that only exists in Redis during active processing:

- **Database States**: Only persistent states like `PENDING`, `COMPLETED`, `FAILED`
- **Redis Processing**: Temporary tracking during processing (automatic cleanup)
- **Crash Recovery**: Items won't get stuck in processing state after system crashes
- **Performance**: No database writes for temporary state changes

```javascript
static
get
STATES()
{
    return {
        PENDING: 1,    // Database: Ready to process
        COMPLETED: 2,  // Database: Successfully processed
        FAILED: 3,     // Database: Processing failed
        // PROCESSING state exists only in Redis (virtual)
    };
}
```

### Criteria-Based Selection

Producers now use a criteria object instead of just a state number. This allows for more flexible item selection:

```javascript
// Simple state-based criteria
const criteria = {state: STATES.PENDING};

// Complex criteria
const criteria = {
    state: STATES.PENDING,
    priority: "high",
    category: "urgent",
    createdBefore: new Date("2024-01-01"),
};
```

### Abstract Methods

#### Consumer Abstract Methods

- `getMessageId(messageData)` - Extract unique ID from message
- `getMessageKey(messageData)` - Extract display key for logging
- `process(messageData)` - Your business logic
- `markItemAsCompleted(itemId)` - Mark item as completed
- `markItemAsFailed(itemId)` - Mark item as failed
- `isItemCompleted(itemId)` - Check if item is already completed

> **Note**: `markItemAsProcessing` is no longer required. Processing state is now virtual (Redis-only) and handled
> automatically by the framework.

#### Producer Abstract Methods

- `getNextProcessingItems(criteria, limit, excludedIds)` - Fetch items to process
- `getItemId(item)` - Extract unique ID from item
- `getItemKey(item)` - Extract display key for logging

### Hook Methods (Optional)

Both consumers and producers provide optional hook methods you can override:

- `onSuccess(itemId)` - Called when processing succeeds
- `onFailed(itemId, error)` - Called when processing fails
- `handleProcessingResult(itemId, result)` - (Consumer only) Handle processing results

## Configuration Guide

### Full Configuration Options

#### Consumer Configuration

```javascript
const consumerConfig = {
    // Required fields
    topic: "my-topic", // Kafka topic name
    consumerGroup: "my-group", // Consumer group ID
    redisKeyPrefix: "MY_APP", // Redis key prefix for tracking

    // Optional: Topic configuration
    topicOptions: {
        partitions: 4, // Number of partitions
        replicationFactor: 2, // Replication factor
        configEntries: {
            // Kafka topic configurations
            "retention.ms": "604800000", // Message retention
            "compression.type": "gzip", // Compression
            "max.message.bytes": "1048576", // Max message size
        },
    },
};
```

#### Producer Configuration

```javascript
const producerConfig = {
    // Required fields
    topic: "my-topic", // Kafka topic name
    redisKeyPrefix: "MY_APP", // Redis key prefix

    // Optional: Same topicOptions as consumer
    topicOptions: {
        /* ... */
    },
};
```

### Environment Variables

- `MAX_CONCURRENT_MESSAGES` - Consumer concurrent processing (default: 3)
- `PARTITIONS_CONSUMED_CONCURRENTLY` - Consumer partition concurrency (default: 1)
- `STATUS_REPORT_INTERVAL` - Status report interval ms (default: 30000)
- `LOG_LEVEL` - Set to 'debug' for detailed producer logs
- `KAFKA_TOPIC_PARTITIONS` - Default partition count (default: 1)
- `KAFKA_REPLICATION_FACTOR` - Default replication (default: 1)

## Examples

See the included examples:

- **Crawler Example** (`crawler/`) - Web scraping implementation
- **Email Example** (`email/`) - Email sending implementation

Each example demonstrates:

- State constant definitions
- Abstract method implementations
- Integration with data sources
- Error handling patterns
