const { kafkaProducer, KafkaClientFactory } = require("../client/kafka");
const RedisService = require("../services/redisService");
const TopicService = require("../services/topicService");
const MessageService = require("../services/messageService");
const logger = require("../services/loggerService");

/**
 * Abstract Producer Base Class
 *
 * Provides common producer functionality for any data source including:
 * - Redis deduplication and processing tracking
 * - Kafka message sending with hybrid deduplication
 * - Connection management
 * - Abstract methods for business logic
 *
 * Works with any data source that implements the required abstract methods
 */
class AbstractProducer {
  /**
   * Constructor for AbstractProducer
   * @param {Object} config - Configuration object
   * @param {string} config.topic - Kafka topic name
   * @param {Object} config.redisOptions - Redis configuration options for tracking
   * @param {string} config.redisOptions.keyPrefix - Redis key prefix for deduplication tracking (required)
   * @param {number} [config.redisOptions.processingTtl] - TTL for processing keys in seconds (default: 10)
   * @param {number} [config.redisOptions.sentTtl] - TTL for sent message keys in seconds (default: 20)
   * @param {Object} [config.topicOptions] - Optional topic configuration
   * @param {number} [config.topicOptions.partitions] - Number of partitions (default: from env KJO_KAFKA_TOPIC_PARTITIONS || 4)
   * @param {number} [config.topicOptions.replicationFactor] - Replication factor (default: from env KJO_KAFKA_TOPIC_REPLICATION_FACTOR || 1)
   * @param {boolean} [config.topicOptions.autoCreate] - Auto-create topic if doesn't exist (default: from env KJO_KAFKA_TOPIC_AUTO_CREATE_TOPICS || false)
   * @param {string} [config.topicOptions.retentionMs] - Topic retention time (default: from env KJO_KAFKA_TOPIC_RETENTION_MS || '604800000')
   * @param {string} [config.topicOptions.segmentMs] - Topic segment time (default: from env KJO_KAFKA_TOPIC_SEGMENT_MS || '86400000')
   * @param {string} [config.topicOptions.compressionType] - Topic compression type (default: from env KJO_KAFKA_TOPIC_COMPRESSION_TYPE || 'producer')
   * @param {string} [config.topicOptions.cleanupPolicy] - Topic cleanup policy (default: from env KJO_KAFKA_TOPIC_CLEANUP_POLICY || 'delete')
   * @param {Object} [config.topicOptions.configEntries] - Additional Kafka topic configuration entries
   *
   * @example
   * // Minimal configuration (required fields only)
   * const minimalConfig = {
   *   topic: 'product-updates',
   *   redisOptions: {
   *     keyPrefix: 'PRODUCT_UPDATES'
   *   }
   * };
   *
   * // Complete configuration with all options
   * const fullConfig = {
   *   // Required fields
   *   topic: 'inventory-sync',
   *   redisOptions: {
   *     keyPrefix: 'INVENTORY_SYNC',      // Required: Redis key prefix
   *     processingTtl: 180,               // Optional: 3 minutes processing TTL
   *     sentTtl: 360                      // Optional: 6 minutes sent TTL
   *   },
   *
   *   // Optional: Topic configuration (creates/configures topic if doesn't exist)
   *   topicOptions: {
   *     partitions: 12,                   // Number of topic partitions for high throughput
   *     replicationFactor: 3,             // Replication factor for high availability
   *     autoCreate: true,                 // Auto-create topic if it doesn't exist
   *     retentionMs: '1209600000',        // 14 days retention
   *     segmentMs: '86400000',            // 1 day segment time
   *     compressionType: 'lz4',           // Fast compression for high throughput
   *     cleanupPolicy: 'delete',          // Cleanup policy
   *     configEntries: {                  // Additional Kafka topic configurations
   *       'max.message.bytes': '5242880',          // 5MB max message size
   *       'min.insync.replicas': '2',              // Minimum in-sync replicas
   *       'unclean.leader.election.enable': 'false', // Prevent data loss
   *       'compression.type': 'lz4',               // Message compression
   *       'segment.bytes': '1073741824',           // 1GB segment size
   *       'flush.messages': '10000',               // Flush every 10k messages
   *       'flush.ms': '1000'                       // Flush every 1 second
   *     }
   *   }
   * };
   *
   * // Usage examples:
   * class InventoryProducer extends AbstractProducer {
   *   constructor() {
   *     super(fullConfig);
   *   }
   *
   *   async getNextProcessingItems(criteria, limit, excludedIds) {
   *     // Implement your data fetching logic here
   *     return await this.inventoryService.findPendingInventory(criteria, limit, excludedIds);
   *   }
   *
   *   getItemId(item) {
   *     return item._id.toString();
   *   }
   *
   *   getItemKey(item) {
   *     return item.productSku;
   *   }
   * }
   *
   * // Environment variables that affect behavior (can override defaults):
   * // KJO_KAFKA_BROKERS - Kafka broker connection string
   * // KJO_KAFKA_CLIENT_ID - Kafka client identifier
   * // KJO_KAFKA_PRODUCER_RETRY_RETRIES - Producer retry attempts (default: 5)
   * // KJO_KAFKA_PRODUCER_RETRY_INITIAL_RETRY_TIME - Initial retry delay (default: 300)
   * // KJO_REDIS_KEY_PREFIX - Default Redis key prefix (default: 'SUPERNOVA')
   * // KJO_REDIS_PROCESSINGTTL_SECONDS - Default processing TTL (default: 10)
   * // KJO_REDIS_SENT_TTL_SECONDS - Default sent TTL (default: 20)
   */
  constructor(config) {
    this._validateConfiguration(config);
    this._preventDirectInstantiation();
    this._initializeServices(config);
    this.isConnected = false;
  }

  static get LOG_LEVELS() {
    return {
      DEBUG: "debug",
      INFO: "info",
      ERROR: "error",
    };
  }

  _validateConfiguration(config) {
    if (!config || typeof config !== "object") {
      throw new Error("Configuration must be an object");
    }

    const requiredFields = ["topic", "redisOptions"];
    this._validateRequiredFields(config, requiredFields);
    this._validateStringFields(config);
  }

  _validateRequiredFields(config, requiredFields) {
    for (const field of requiredFields) {
      if (!(field in config)) {
        throw new Error(`Configuration must include ${field}`);
      }
    }
  }

  _validateStringFields(config) {
    const stringFields = ["topic"];

    for (const field of stringFields) {
      if (!config[field] || typeof config[field] !== "string") {
        throw new Error(`${field} must be a non-empty string`);
      }
    }
  }

  _preventDirectInstantiation() {
    if (this.constructor === AbstractProducer) {
      throw new Error("AbstractProducer cannot be instantiated directly");
    }
  }

  _initializeServices(config) {
    this.topic = config.topic;

    this.topicService = new TopicService(config.topicOptions || {});
    this.redisService = new RedisService(config.redisOptions);
    this.messageService = new MessageService();
  }

  async connect() {
    try {
      await this._connectToAllServices();
      this.isConnected = true;
      this._logSuccessfulConnection();
    } catch (error) {
      this.isConnected = false;
      throw new Error(`Failed to connect producer: ${error.message}`);
    }
  }

  async _connectToAllServices() {
    await this._ensureTopicExists();
    await this.redisService.connect();
    await kafkaProducer.connect();
  }

  async _ensureTopicExists() {
    try {
      const result = await this.topicService.ensureTopicExists(this.topic);
      logger.logInfo(
        `📊 Topic '${this.topic}' ready with ${result.partitions} partitions`
      );
    } catch (error) {
      logger.logWarning(
        `⚠️ Could not configure topic '${this.topic}': ${error.message}`
      );
    }
  }

  _logSuccessfulConnection() {
    logger.logConnectionEvent(
      this.constructor.name,
      "connected to all services"
    );
  }

  async produceMessages(criteria, limit, messageType) {
    this._ensureProducerIsConnected();

    try {
      const itemsToProcess = await this.getNextBatch(criteria, limit);

      if (this._hasNoItemsToProcess(itemsToProcess)) {
        return;
      }

      const batchResult = await this._processBatchAndSendMessages(
        itemsToProcess,
        messageType
      );
      this._logBatchSuccess(batchResult, messageType);
    } catch (error) {
      await this._handleBatchProcessingError(error);
      throw error;
    }
  }

  _ensureProducerIsConnected() {
    if (!this.isConnected) {
      throw new Error("Producer must be connected before sending messages");
    }
  }

  async getNextBatch(criteria, limit) {
    this._validateBatchParameters(criteria, limit);

    try {
      this._logBatchStart(criteria, limit);
      const excludedIds = await this._getExcludedItemIds();
      const items = await this._fetchItemsFromDataSource(
        criteria,
        limit,
        excludedIds
      );

      if (this._hasNoItemsToProcess(items)) {
        return [];
      }

      const uniqueItems = await this._filterForUniqueItems(items);
      return this._validateAndReturnBatch(uniqueItems);
    } catch (error) {
      logger.logError("Error getting next batch", error);
      throw error;
    }
  }

  _validateBatchParameters(criteria, limit) {
    if (criteria === null || criteria === undefined) {
      throw new Error("Criteria must be provided");
    }
    if (typeof limit !== "number" || limit <= 0) {
      throw new Error("Limit must be a positive number");
    }
  }

  _logBatchStart(criteria, limit) {
    logger.logInfo(
      `Getting next batch (criteria=${JSON.stringify(criteria)}, limit=${limit})`
    );
  }

  async _getExcludedItemIds() {
    const processingIds = await this.redisService.getProcessingIds();
    const sentIds = await this.redisService.getSentIds();
    const excludedIds = [...new Set([...processingIds, ...sentIds])];

    this._logExcludedIds(processingIds, sentIds);
    return excludedIds;
  }

  _logExcludedIds(processingIds, sentIds) {
    logger.logDebug("Excluding processing IDs", { processingIds });
    logger.logDebug("Excluding recently sent IDs", { sentIds });
  }

  async _fetchItemsFromDataSource(criteria, limit, excludedIds) {
    const items = await this.getNextProcessingItems(
      criteria,
      limit,
      excludedIds
    );
    logger.logInfo(`Found ${items.length} items to process`);
    return items;
  }

  _hasNoItemsToProcess(items) {
    if (!items || items.length === 0) {
      logger.logInfo(
        "No items found to process (all are either processing or recently sent)"
      );
      return true;
    }
    return false;
  }

  async _filterForUniqueItems(items) {
    const uniqueItems = [];

    for (const item of items) {
      if (await this._isItemUniqueForProcessing(item)) {
        const messageHash = this.redisService.generateMessageHash(item);
        uniqueItems.push({ item, messageHash });
      }
    }

    return uniqueItems;
  }

  async _isItemUniqueForProcessing(item) {
    const messageHash = this.redisService.generateMessageHash(item);
    const isRecentlySent = await this.redisService.isSentRecently(
      this.getItemId(item),
      messageHash
    );

    if (isRecentlySent) {
      logger.logDebug(
        `Skipping duplicate message for item: ${this.getItemKey(item)}`
      );
      return false;
    }

    return true;
  }

  _validateAndReturnBatch(uniqueItems) {
    if (uniqueItems.length === 0) {
      logger.logInfo("No unique items to send (all are duplicates)");
    }
    return uniqueItems;
  }

  async _processBatchAndSendMessages(itemsToProcess, messageType) {
    const batchId = this.messageService.generateBatchId();
    const messages = this._createKafkaMessagesForBatch(
      itemsToProcess,
      messageType
    );

    this._logBatchProcessingStart(batchId, messages.length);

    const sendResult = await this._sendMessagesToKafka(messages);
    await this._markMessagesAsSent(itemsToProcess);

    return { batchId, messages, result: sendResult };
  }

  _createKafkaMessagesForBatch(itemsToProcess, messageType) {
    const itemsWithHashes = this._convertToGenericFormat(itemsToProcess);
    return this.messageService.createKafkaMessages(
      itemsWithHashes,
      messageType
    );
  }

  _convertToGenericFormat(itemsToProcess) {
    return itemsToProcess.map(({ item, messageHash }) => ({
      data: item,
      messageHash,
    }));
  }

  _logBatchProcessingStart(batchId, messageCount) {
    logger.logBatchOperation(
      batchId,
      `🔄 Sending ${messageCount} messages with hybrid deduplication`
    );
  }

  async _sendMessagesToKafka(messages) {
    const sendOptions = KafkaClientFactory.getSendOptions();

    return await kafkaProducer.send({
      topic: this.topic,
      messages,
      acks: sendOptions.acks,
      timeout: sendOptions.timeout,
      compression: sendOptions.compression,
    });
  }

  async _markMessagesAsSent(itemsToProcess) {
    for (const { item, messageHash } of itemsToProcess) {
      const itemId = this.getItemId(item);
      await this.redisService.setSentMessage(itemId, messageHash);
      await this.onSuccess(itemId);
    }
  }

  _logBatchSuccess({ batchId, messages, result }, messageType) {
    const summary = this.messageService.analyzeResult(result, messages.length);

    logger.logBatchOperation(
      batchId,
      `✅ Successfully sent ${summary.successful} ${messageType} messages to Kafka topic: ${this.topic}`
    );

    this._logDebugSummaryIfEnabled(batchId, summary);
  }

  _logDebugSummaryIfEnabled(batchId, summary) {
    if (logger.isDebugEnabled()) {
      logger.logBatchOperation(batchId, `📊 Summary`, summary);
    }
  }

  async _handleBatchProcessingError(error) {
    logger.logError("Error producing messages", error);
    await this.onFailed(null, error);
  }

  async disconnect() {
    try {
      await this._disconnectFromAllServices();
      this.isConnected = false;
    } catch (error) {
      logger.logError("Error during disconnect", error);
    }
  }

  async _disconnectFromAllServices() {
    await kafkaProducer.disconnect();
    await this.redisService.disconnect();
  }

  /**
   * Abstract Methods - Must be implemented by concrete producers
   */

  /**
   * Get next processing items from data source
   * @param {*} criteria - Search criteria
   * @param {number} limit - Maximum number of items to fetch
   * @param {Array<string>} excludedIds - IDs to exclude from results
   * @returns {Promise<Array>} Array of items to process
   */
  async getNextProcessingItems(criteria, limit, excludedIds) {
    throw new Error(
      "getNextProcessingItems method must be implemented by concrete producer"
    );
  }

  /**
   * Get unique identifier for an item
   * @param {Object} item - The item object
   * @returns {string} Unique identifier for the item
   */
  getItemId(item) {
    throw new Error(
      "getItemId method must be implemented by concrete producer"
    );
  }

  /**
   * Get display key for an item (for logging)
   * @param {Object} item - The item object
   * @returns {string} Display-friendly key for the item
   */
  getItemKey(item) {
    throw new Error(
      "getItemKey method must be implemented by concrete producer"
    );
  }

  /**
   * Hook Methods - Can be overridden by concrete producers
   */

  /**
   * Called when an item is successfully sent
   * @param {string} itemId - Item identifier
   */
  async onSuccess(itemId) {
    logger.logInfo(`✅ Producer: Successfully processed item ID: ${itemId}`);
  }

  /**
   * Called when an item fails to send
   * @param {string} itemId - Item identifier
   * @param {Error} error - The error that occurred
   */
  async onFailed(itemId, error) {
    logger.logError(`❌ Producer: Failed to process item ID: ${itemId}`, error);
  }
}

module.exports = AbstractProducer;
