/**
 * Kafka Job Orchestrator (KJO)
 *
 * High-availability distributed job orchestrator with state management
 * Built on top of KafkaJS with Redis-based deduplication
 *
 * Main Features:
 * - Abstract base classes for producers and consumers
 * - Concurrent message processing with prefetch control
 * - Redis-based deduplication and state tracking
 * - Kafka topic management with partition scaling
 * - Production-ready client connections
 */

// ==================== ABSTRACT BASE CLASSES ====================
// Core abstractions that users extend to implement their business logic
const AbstractProducer = require("./abstracts/AbstractProducer");
const AbstractConsumer = require("./abstracts/AbstractConsumer");

// ==================== SERVICES ====================
// Utility services for advanced customization and control
const ConcurrentProcessor = require("./services/concurrentProcessor");
const MessageService = require("./services/messageService");
const RedisService = require("./services/redisService");
const TopicService = require("./services/topicService");

// ==================== CLIENT CONNECTIONS ====================
// Pre-configured client connections (optional - users can provide their own)
const kafkaClients = require("./client/kafka");
const redisClients = require("./client/redis");

// ==================== MAIN EXPORTS ====================
const KafkaJobOrchestrator = {
  // Primary exports - abstract classes for implementing producers/consumers
  AbstractProducer,
  AbstractConsumer,

  // Service classes for advanced users
  services: {
    ConcurrentProcessor,
    MessageService,
    RedisService,
    TopicService,
  },

  // Optional client connections
  clients: {
    kafka: kafkaClients,
    redis: redisClients,
  },
};

// ==================== COMMONJS EXPORTS ====================
module.exports = KafkaJobOrchestrator;

// ==================== NAMED EXPORTS FOR ES6 MODULES ====================
// Support both CommonJS and ES6 import styles
module.exports.AbstractProducer = AbstractProducer;
module.exports.AbstractConsumer = AbstractConsumer;
module.exports.ConcurrentProcessor = ConcurrentProcessor;
module.exports.MessageService = MessageService;
module.exports.RedisService = RedisService;
module.exports.TopicService = TopicService;

// Export the entire namespace as default for ES6
module.exports.default = KafkaJobOrchestrator;
