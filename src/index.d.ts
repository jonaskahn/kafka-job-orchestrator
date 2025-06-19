/**
 * Kafka Job Orchestrator (KJO) TypeScript Definitions
 *
 * Type definitions for the distributed job orchestrator
 */

/// <reference types="node" />

import { Kafka, Producer } from "kafkajs";
import { RedisClientType } from "redis";

// ==================== CONFIGURATION INTERFACES ====================

/**
 * Redis configuration options
 */
export interface RedisOptions {
  /** Redis key prefix for deduplication tracking */
  keyPrefix: string;
  /** TTL for processing keys in seconds */
  processingTtl?: number;
  /** TTL for sent message keys in seconds */
  sentTtl?: number;
}

/**
 * Configuration for AbstractProducer
 */
export interface ProducerConfig {
  /** Kafka topic name */
  topic: string;
  /** Redis configuration options */
  redisOptions: RedisOptions;
  /** Optional topic configuration */
  topicOptions?: TopicOptions;
}

/**
 * Configuration for AbstractConsumer
 */
export interface ConsumerConfig {
  /** Kafka topic name */
  topic: string;
  /** Kafka consumer group ID */
  consumerGroup: string;
  /** Redis configuration options */
  redisOptions: RedisOptions;
  /** Optional topic configuration */
  topicOptions?: TopicOptions;
}

/**
 * Kafka topic configuration options
 */
export interface TopicOptions {
  /** Number of partitions */
  partitions?: number;
  /** Replication factor */
  replicationFactor?: number;
  /** Additional Kafka topic configuration entries */
  configEntries?: Record<string, string>;
  /** Auto-create topics if they don't exist */
  autoCreate?: boolean;
  /** Retention time in milliseconds */
  retentionMs?: string;
  /** Segment time in milliseconds */
  segmentMs?: string;
  /** Compression type */
  compressionType?: string;
  /** Cleanup policy */
  cleanupPolicy?: string;
}

// ==================== DATA INTERFACES ====================

/**
 * Message data structure for processing
 */
export interface MessageData {
  /** Unique identifier */
  id: string;
  /** Actual message payload */
  data: any;
  /** Optional message hash for deduplication */
  messageHash?: string;

  /** Allow additional properties */
  [key: string]: any;
}

/**
 * Result of message processing
 */
export interface ProcessingResult {
  /** Whether processing was successful */
  success: boolean;
  /** Optional result data */
  data?: any;
  /** Error if processing failed */
  error?: Error;
}

/**
 * Concurrent processing status information
 */
export interface ConcurrentStatus {
  /** Current active tasks */
  activeTasks: number;
  /** Number of queued tasks */
  queueLength: number;
  /** Maximum allowed concurrency */
  maxConcurrency: number;
  /** Total processed messages */
  totalProcessed: number;
  /** Successfully processed messages */
  totalSuccessful: number;
  /** Failed messages */
  totalFailed: number;
}

/**
 * Detailed concurrent processing status
 */
export interface DetailedConcurrentStatus {
  /** Concurrency information */
  concurrency: {
    max: number;
    active: number;
    queued: number;
    available: number;
  };
  /** Performance metrics */
  performance: {
    processed: number;
    failed: number;
    successRate: string;
    maxConcurrentReached: number;
  };
  /** Queue status */
  queue: {
    length: number;
    oldestWaitingMs: number;
  };
}

// ==================== ABSTRACT CLASSES ====================

/**
 * Abstract base class for Kafka producers
 * Extend this class to implement your producer logic
 */
export abstract class AbstractProducer {
  protected topic: string;
  protected redisOptions: RedisOptions;
  protected isConnected: boolean;

  protected constructor(config: ProducerConfig);

  /** Connect to all required services */
  connect(): Promise<void>;

  /** Disconnect from all services */
  disconnect(): Promise<void>;

  /** Produce messages based on criteria */
  produceMessages(
    criteria: any,
    limit: number,
    messageType: string
  ): Promise<void>;

  /** Get next batch of items to process */
  getNextBatch(
    criteria: any,
    limit: number
  ): Promise<Array<{ item: any; messageHash: string }>>;

  /** Abstract: Get next processing items from data source */
  abstract getNextProcessingItems(
    criteria: any,
    limit: number,
    excludedIds: string[]
  ): Promise<any[]>;

  /** Abstract: Extract unique ID from item */
  abstract getItemId(item: any): string;

  /** Abstract: Extract display key from item */
  abstract getItemKey(item: any): string;

  /** Hook: Called on successful processing */
  onSuccess(itemId: string): Promise<void>;

  /** Hook: Called on failed processing */
  onFailed(itemId: string, error: Error): Promise<void>;
}

/**
 * Abstract base class for Kafka consumers
 * Extend this class to implement your consumer logic
 */
export abstract class AbstractConsumer {
  protected topic: string;
  protected consumerGroup: string;
  protected redisOptions: RedisOptions;
  protected isConnected: boolean;
  protected maxConcurrency: number;

  constructor(config: ConsumerConfig);

  /** Connect to all required services */
  connect(): Promise<void>;

  /** Disconnect from all services */
  disconnect(): Promise<void>;

  /** Start consuming messages */
  startConsuming(): Promise<void>;

  /** Get concurrent processing status */
  getConcurrentStatus(): DetailedConcurrentStatus;

  /** Update max concurrency at runtime */
  setMaxConcurrency(newMaxConcurrency: number): void;

  /** Show processing statistics */
  showStats(): void;

  /** Abstract: Extract message ID */
  abstract getMessageId(messageData: MessageData): Promise<string> | string;

  /** Abstract: Extract message key for logging */
  abstract getMessageKey(messageData: MessageData): Promise<string> | string;

  /** Abstract: Main processing logic */
  abstract process(messageData: MessageData): Promise<any>;

  /** Abstract: Mark item as completed in database */
  abstract markItemAsCompleted(itemId: string): Promise<void>;

  /** Abstract: Mark item as failed in database */
  abstract markItemAsFailed(itemId: string): Promise<void>;

  /** Abstract: Check if item is completed */
  abstract isItemCompleted(itemId: string): Promise<boolean>;

  /** Hook: Handle processing result */
  handleProcessingResult(id: string, result: any): Promise<void>;

  /** Hook: Called on successful processing */
  onSuccess(id: string): Promise<void>;

  /** Hook: Called on failed processing */
  onFailed(id: string, error: Error): Promise<void>;
}

// ==================== SERVICE CLASSES ====================

/**
 * Concurrent processor for handling multiple tasks
 * Implements RabbitMQ-like prefetch mechanism
 */
export class ConcurrentProcessor {
  constructor(maxConcurrency?: number);

  /** Process a message with concurrency control */
  processMessage<T>(
    messageData: any,
    processingFunction: (data: any) => Promise<T>
  ): Promise<T>;

  /** Update maximum concurrency dynamically */
  setMaxConcurrency(newMaxConcurrency: number): void;

  /** Get basic processing statistics */
  getStats(): ConcurrentStatus;

  /** Get detailed processing status */
  getDetailedStatus(): DetailedConcurrentStatus;

  /** Wait for all tasks to complete */
  waitForCompletion(): Promise<void>;

  /** Check if processor is at capacity */
  isAtCapacity(): boolean;
}

/**
 * Message service for Kafka message handling
 * Handles message creation and deduplication
 */
export class MessageService {
  constructor();

  /** Generate unique batch identifier */
  generateBatchId(): string;

  /** Generate stable message key for Kafka idempotent producer */
  generateStableMessageKey(itemData: any, messageType: string): string;

  /** Generate message hash for Redis deduplication */
  generateRedisHash(itemData: any): string;

  /** Create Kafka message with enhanced headers */
  createKafkaMessage(
    itemData: any,
    messageType: string,
    stableKey: string
  ): any;

  /** Create batch of Kafka messages */
  createKafkaMessages(
    itemsWithHashes: Array<{ data: any; messageHash: string }>,
    messageType: string
  ): any[];

  /** Analyze message sending result */
  analyzeResult(
    result: any,
    totalMessages: number
  ): {
    totalMessages: number;
    successful: number;
    failed: number;
    duplicatesRejected: number;
    brokerResponse?: any;
  };

  /** Log detailed message information for debugging */
  logMessageDetails(messages: any[], batchId: string): void;
}

/**
 * Redis service for deduplication and state tracking
 * Manages processing state and prevents duplicates
 */
export class RedisService {
  /** Connection status - property not getter */
  isConnected: boolean;

  constructor(redisOptions: RedisOptions);

  /** Connect to Redis */
  connect(): Promise<void>;

  /** Disconnect from Redis */
  disconnect(): Promise<void>;

  /** Get all processing item IDs */
  getProcessingIds(): Promise<string[]>;

  /** Get all recently sent item IDs */
  getSentIds(): Promise<string[]>;

  /** Check if item was sent recently with same content */
  isSentRecently(itemId: string, messageHash: string): Promise<boolean>;

  /** Set sent message tracking */
  setSentMessage(itemId: string, messageHash: string): Promise<void>;

  /** Get sent message hash */
  getSentMessageHash(itemId: string): Promise<string | null>;

  /** Set processing key for an item */
  setProcessingKey(itemId: string): Promise<void>;

  /** Remove processing key for an item */
  removeProcessingKey(itemId: string): Promise<void>;

  /** Check if item is currently being processed */
  isProcessing(itemId: string): Promise<boolean>;

  /** Generate message hash for deduplication */
  generateMessageHash(item: any): string;
}

/**
 * Topic service for Kafka topic management
 * Handles topic creation and partition management
 */
export class TopicService {
  constructor(options?: TopicOptions);

  /** Ensure topic exists with specified configuration */
  ensureTopicExists(
    topicName: string,
    partitionCount?: number,
    topicConfig?: Record<string, any>
  ): Promise<{
    created: boolean;
    partitions: number;
    existed: boolean;
  }>;

  /** Get topic partition count */
  getTopicPartitionCount(topicName: string): Promise<number>;

  /** Configure topic for specific use case */
  configureTopicForUseCase(
    topicName: string,
    useCase: "high-throughput" | "low-latency" | "development" | "production"
  ): Promise<{
    created: boolean;
    partitions: number;
    existed: boolean;
  }>;

  /** List all topics with partition information */
  listTopicsWithPartitions(): Promise<
    Array<{
      name: string;
      partitions: number;
      replicas: number;
    }>
  >;
}

// ==================== CLIENT FACTORY INTERFACES ====================

/**
 * Kafka client factory interface
 */
export interface KafkaClientFactory {
  /** Create a new Kafka client */
  createClient(): Kafka;

  /** Create a new Kafka producer */
  createProducer(kafkaClient: Kafka): Producer;

  /** Get default send options */
  getSendOptions(): {
    acks: number;
    timeout: number;
    compression?: string;
  };

  /** Validate producer configuration */
  validateProducerConfiguration(): void;
}

/**
 * Redis client factory interface
 */
export interface RedisClientFactory {
  /** Create a new Redis client */
  createClient(): RedisClientType;
}

// ==================== CLIENT EXPORTS ====================

/**
 * Kafka client module exports
 */
export interface KafkaClients {
  /** Singleton Kafka client instance */
  kafkaClient: Kafka;
  /** Singleton Kafka producer instance */
  kafkaProducer: Producer;
  /** Factory class for creating clients */
  KafkaClientFactory: KafkaClientFactory;
}

/**
 * Redis client module exports
 */
export interface RedisClients {
  /** Singleton Redis client instance */
  redisClient: RedisClientType;
  /** Factory class for creating clients */
  RedisClientFactory: RedisClientFactory;
}

// ==================== MAIN MODULE INTERFACE ====================

/**
 * Main module interface matching actual exports
 */
export interface KafkaJobOrchestrator {
  /** Abstract producer class */
  AbstractProducer: typeof AbstractProducer;
  /** Abstract consumer class */
  AbstractConsumer: typeof AbstractConsumer;

  /** Service classes */
  services: {
    ConcurrentProcessor: typeof ConcurrentProcessor;
    MessageService: typeof MessageService;
    RedisService: typeof RedisService;
    TopicService: typeof TopicService;
  };

  /** Client connections */
  clients: {
    kafka: KafkaClients;
    redis: RedisClients;
  };
}

// ==================== MODULE EXPORTS ====================

/** Default export - the main module object */
declare const _default: KafkaJobOrchestrator;
export default _default;

/** Named exports for individual classes */
export {
  AbstractProducer,
  AbstractConsumer,
  ConcurrentProcessor,
  MessageService,
  RedisService,
  TopicService,
};

/** Additional type exports */
export {
  ProducerConfig,
  ConsumerConfig,
  TopicOptions,
  RedisOptions,
  MessageData,
  ProcessingResult,
  ConcurrentStatus,
  DetailedConcurrentStatus,
  KafkaClients,
  RedisClients,
  KafkaClientFactory,
  RedisClientFactory,
};
