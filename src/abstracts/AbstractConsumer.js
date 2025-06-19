const { kafkaClient } = require("../client/kafka");
const RedisService = require("../services/redisService");
const ConcurrentProcessor = require("../services/concurrentProcessor");
const TopicService = require("../services/topicService");
const logger = require("../services/loggerService");

/**
 * Abstract Consumer Base Class
 *
 * Provides common consumer functionality for any data source including:
 * - Official KafkaJS consumer configuration
 * - Redis processing key management
 * - Concurrent message processing with partition awareness
 * - Connection management with proper error handling
 * - Status reporting and monitoring
 * - Abstract methods for business logic
 *
 * Works with any data source that implements the required abstract methods
 */
class AbstractConsumer {
  /**
   * Constructor for AbstractConsumer
   * @param {Object} config - Configuration object
   * @param {string} config.topic - Kafka topic name
   * @param {string} config.consumerGroup - Kafka consumer group ID
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
   *   topic: 'user-events',
   *   consumerGroup: 'user-events-processor',
   *   redisOptions: {
   *     keyPrefix: 'USER_EVENTS'
   *   }
   * };
   *
   * // Complete configuration with all options
   * const fullConfig = {
   *   // Required fields
   *   topic: 'order-processing',
   *   consumerGroup: 'order-processor-group',
   *   redisOptions: {
   *     keyPrefix: 'ORDER_PROCESSOR',     // Required: Redis key prefix
   *     processingTtl: 300,               // Optional: 5 minutes processing TTL
   *     sentTtl: 600                      // Optional: 10 minutes sent TTL
   *   },
   *
   *   // Optional: Topic configuration (creates/configures topic if doesn't exist)
   *   topicOptions: {
   *     partitions: 8,                    // Number of topic partitions
   *     replicationFactor: 3,             // Replication factor for high availability
   *     autoCreate: true,                 // Auto-create topic if it doesn't exist
   *     retentionMs: '2592000000',        // 30 days retention
   *     segmentMs: '86400000',            // 1 day segment time
   *     compressionType: 'snappy',        // Compression type
   *     cleanupPolicy: 'delete',          // Cleanup policy
   *     configEntries: {                  // Additional Kafka topic configurations
   *       'max.message.bytes': '2097152',          // 2MB max message size
   *       'min.insync.replicas': '2',              // Minimum in-sync replicas
   *       'unclean.leader.election.enable': 'false', // Prevent data loss
   *       'compression.type': 'snappy',             // Message compression
   *       'delete.retention.ms': '86400000'        // 1 day delete retention
   *     }
   *   }
   * };
   *
   * // Usage examples:
   * class OrderConsumer extends AbstractConsumer {
   *   constructor() {
   *     super(fullConfig);
   *   }
   * }
   *
   * // Environment variables that affect behavior (can override defaults):
   * // KJO_CONSUMSER_MAX_CONCURRENT_MESSAGES - Concurrent message processing limit (default: 3)
   * // KJO_CONSUMSER_PARTITIONS_CONSUMED_CONCURRENTLY - Concurrent partition consumption (default: 1)
   * // KJO_CONSUMSER_STATUS_REPORT_INTERVAL_MS - Status reporting interval (default: 30000)
   * // KJO_CONSUMSER_SESSION_TIMEOUT_MS - Kafka session timeout (default: 30000)
   * // KJO_CONSUMSER_HEARTBEAT_INTERVAL_MS - Kafka heartbeat interval (default: 3000)
   * // KJO_REDIS_KEY_PREFIX - Default Redis key prefix (default: 'SUPERNOVA')
   * // KJO_REDIS_PROCESSINGTTL_SECONDS - Default processing TTL (default: 10)
   * // KJO_REDIS_SENT_TTL_SECONDS - Default sent TTL (default: 20)
   */
  constructor(config) {
    this._validateConfiguration(config);
    this._preventDirectInstantiation();
    this._initializeServices(config);
    this._initializeConcurrentProcessing();
    this.isConnected = false;
  }

  static get CONFIGURATION_DEFAULTS() {
    return {
      SESSION_TIMEOUT_MS:
        parseInt(process.env.KJO_CONSUMSER_SESSION_TIMEOUT_MS) || 30000,
      REBALANCE_TIMEOUT_MS:
        parseInt(process.env.KJO_CONSUMSER_REBALANCE_TIMEOUT_MS) || 60000,
      HEARTBEAT_INTERVAL_MS:
        parseInt(process.env.KJO_CONSUMSER_HEARTBEAT_INTERVAL_MS) || 3000,
      METADATA_MAX_AGE_MS:
        parseInt(process.env.KJO_CONSUMSER_METADATA_MAX_AGE_MS) || 300000,
      ALLOW_AUTO_TOPIC_CREATION:
        process.env.KJO_CONSUMSER_ALLOW_AUTO_TOPIC_CREATION === "true",
      MAX_BYTES_PER_PARTITION:
        parseInt(process.env.KJO_CONSUMSER_MAX_BYTES_PER_PARTITION) || 1048576,
      MIN_BYTES: parseInt(process.env.KJO_CONSUMSER_MIN_BYTES) || 1,
      MAX_BYTES: parseInt(process.env.KJO_CONSUMSER_MAX_BYTES) || 10485760,
      MAX_WAIT_TIME_MS:
        parseInt(process.env.KJO_CONSUMSER_MAX_WAIT_TIME_MS) || 5000,
      MAX_IN_FLIGHT_REQUESTS:
        parseInt(process.env.KJO_CONSUMSER_MAX_IN_FLIGHT_REQUESTS) || 1,
      MAX_CONCURRENT_MESSAGES:
        parseInt(process.env.KJO_CONSUMSER_MAX_CONCURRENT_MESSAGES) || 3,
      STATUS_REPORT_INTERVAL_MS:
        parseInt(process.env.KJO_CONSUMSER_STATUS_REPORT_INTERVAL_MS) || 30000,
      PARTITIONS_CONSUMED_CONCURRENTLY:
        parseInt(process.env.KJO_CONSUMSER_PARTITIONS_CONSUMED_CONCURRENTLY) ||
        1,
      CONNECTION_RETRY_ATTEMPTS:
        parseInt(process.env.KJO_CONSUMSER_CONNECTION_RETRY_ATTEMPTS) || 5,
      CONNECTION_RETRY_DELAY:
        parseInt(process.env.KJO_CONSUMSER_CONNECTION_RETRY_DELAY) || 2000,
    };
  }

  static get RETRY_CONFIGURATION() {
    return {
      retries: parseInt(process.env.KJO_CONSUMSER_RETRIES) || 10,
      initialRetryTime:
        parseInt(process.env.KJO_CONSUMSER_RETRY_INITIAL_TIME) || 1000,
      maxRetryTime: parseInt(process.env.KJO_CONSUMSER_RETRY_DELAY) || 30000,
      multiplier: parseInt(process.env.KJO_CONSUMSER_RETRY_MULTIPLIER) || 2,
    };
  }

  static get ERROR_RECOVERY_DELAYS() {
    return {
      DEFAULT:
        parseInt(process.env.KJO_CONSUMSER_ERROR_RECOVERY_DELAYS_DEFAULT) ||
        5000,
      NETWORK_ERROR:
        parseInt(process.env.KJO_CONSUMSER_ERROR_RECOVERY_NETWORK_ERROR) ||
        10000,
      BUSINESS_ERROR:
        parseInt(process.env.KJO_CONSUMSER_ERROR_BUSINESS_ERROR) || 2000,
      RATE_LIMIT_ERROR:
        parseInt(process.env.KJO_CONSUMSER_RATE_LIMIT_ERROR) || 30000,
    };
  }

  static get PAUSABLE_ERRORS() {
    return [
      "TooManyRequestsError",
      "TimeoutError",
      "ConnectionError",
      "ECONNREFUSED",
      "ETIMEDOUT",
    ];
  }

  static get BUSINESS_PAUSABLE_ERRORS() {
    return [
      "DatabaseConnectionError",
      "ServiceUnavailableError",
      "RateLimitExceededError",
      "CircuitBreakerOpenError",
    ];
  }

  _validateConfiguration(config) {
    if (!config || typeof config !== "object") {
      throw new Error("Configuration must be an object");
    }

    const requiredFields = ["topic", "consumerGroup", "redisOptions"];
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
    const stringFields = ["topic", "consumerGroup"];

    for (const field of stringFields) {
      if (!config[field] || typeof config[field] !== "string") {
        throw new Error(`${field} must be a non-empty string`);
      }
    }
  }

  _preventDirectInstantiation() {
    if (this.constructor === AbstractConsumer) {
      throw new Error("AbstractConsumer cannot be instantiated directly");
    }
  }

  _initializeServices(config) {
    this.topic = config.topic;
    this.consumerGroup = config.consumerGroup;
    this.topicService = new TopicService(config.topicOptions || {});
    this.redisService = new RedisService(config.redisOptions);
    this.consumer = null;
    this.statusInterval = null;
  }

  _initializeConcurrentProcessing() {
    this.maxConcurrency =
      AbstractConsumer.CONFIGURATION_DEFAULTS.MAX_CONCURRENT_MESSAGES;
    this.partitionsConsumedConcurrently =
      AbstractConsumer.CONFIGURATION_DEFAULTS.PARTITIONS_CONSUMED_CONCURRENTLY;
    this.processor = new ConcurrentProcessor(this.maxConcurrency);
    this._logConcurrentProcessingConfiguration();
  }

  _logConcurrentProcessingConfiguration() {
    logger.logInfo(
      `🔧 ${this.constructor.name} configured: ${this.maxConcurrency} concurrent messages, ${this.partitionsConsumedConcurrently} concurrent partitions`
    );
  }

  async connect() {
    try {
      await this._connectToAllServices();
      this.isConnected = true;
      this._logConnectionSuccess();
    } catch (error) {
      this.isConnected = false;
      throw new Error(`Failed to connect consumer: ${error.message}`);
    }
  }

  async _connectToAllServices() {
    await this.redisService.connect();
    await this._createAndConnectKafkaConsumer();
  }

  async _createAndConnectKafkaConsumer() {
    this._createConsumerWithConfiguration();
    await this._connectConsumerWithRetry();
    await this._subscribeToTopic();
  }

  _createConsumerWithConfiguration() {
    this.consumer = kafkaClient.consumer({
      groupId: this.consumerGroup,
      sessionTimeout:
        AbstractConsumer.CONFIGURATION_DEFAULTS.SESSION_TIMEOUT_MS,
      rebalanceTimeout:
        AbstractConsumer.CONFIGURATION_DEFAULTS.REBALANCE_TIMEOUT_MS,
      heartbeatInterval:
        AbstractConsumer.CONFIGURATION_DEFAULTS.HEARTBEAT_INTERVAL_MS,
      metadataMaxAge:
        AbstractConsumer.CONFIGURATION_DEFAULTS.METADATA_MAX_AGE_MS,
      allowAutoTopicCreation:
        AbstractConsumer.CONFIGURATION_DEFAULTS.ALLOW_AUTO_TOPIC_CREATION,
      maxBytesPerPartition:
        AbstractConsumer.CONFIGURATION_DEFAULTS.MAX_BYTES_PER_PARTITION,
      minBytes: AbstractConsumer.CONFIGURATION_DEFAULTS.MIN_BYTES,
      maxBytes: AbstractConsumer.CONFIGURATION_DEFAULTS.MAX_BYTES,
      maxWaitTimeInMs: AbstractConsumer.CONFIGURATION_DEFAULTS.MAX_WAIT_TIME_MS,
      maxInFlightRequests:
        AbstractConsumer.CONFIGURATION_DEFAULTS.MAX_IN_FLIGHT_REQUESTS,
      retry: AbstractConsumer.RETRY_CONFIGURATION,
      readUncommitted: false,
    });
  }

  async _connectConsumerWithRetry() {
    logger.logConnectionEvent("Consumer", "🔌 Connecting to Kafka");

    const maxRetries =
      AbstractConsumer.CONFIGURATION_DEFAULTS.CONNECTION_RETRY_ATTEMPTS;
    const baseDelay =
      AbstractConsumer.CONFIGURATION_DEFAULTS.CONNECTION_RETRY_DELAY;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        await this.consumer.connect();
        logger.logConnectionEvent("Consumer", "✅ connected successfully");
        return;
      } catch (error) {
        if (this._isLastConnectionAttempt(attempt, maxRetries)) {
          logger.logError(
            `❌ Failed to connect consumer after ${maxRetries} attempts`,
            error
          );
          throw error;
        }

        await this._handleConnectionRetry(
          error,
          attempt,
          maxRetries,
          baseDelay
        );
      }
    }
  }

  _isLastConnectionAttempt(currentAttempt, maxRetries) {
    return currentAttempt === maxRetries;
  }

  async _handleConnectionRetry(error, attempt, maxRetries, baseDelay) {
    const isCoordinatorError = this._isCoordinatorError(error);

    this._logConnectionRetry(error, attempt, maxRetries, isCoordinatorError);

    const delay = this._calculateRetryDelay(
      attempt,
      baseDelay,
      isCoordinatorError
    );
    logger.logInfo(`⏳ Retrying in ${Math.round(delay)}ms...`);

    await new Promise(resolve => setTimeout(resolve, delay));
  }

  _isCoordinatorError(error) {
    return (
      error.message.includes("group coordinator") ||
      error.message.includes("coordinator")
    );
  }

  _logConnectionRetry(error, attempt, maxRetries, isCoordinatorError) {
    if (isCoordinatorError) {
      logger.logWarning(
        `⚠️ Group coordinator issue detected (attempt ${attempt}/${maxRetries})`
      );
    } else {
      logger.logWarning(
        `⚠️ Connection attempt ${attempt}/${maxRetries} failed: ${error.message}`
      );
    }
  }

  _calculateRetryDelay(attempt, baseDelay, isCoordinatorError) {
    return isCoordinatorError
      ? baseDelay * Math.pow(2, attempt - 1) + Math.random() * 1000
      : baseDelay * attempt;
  }

  async _subscribeToTopic() {
    logger.logInfo(`📡 Subscribing to topic: ${this.topic}`);

    try {
      await this.consumer.subscribe({
        topics: [this.topic],
        fromBeginning: false,
      });

      logger.logInfo(`✅ Successfully subscribed to topic: ${this.topic}`);
      await new Promise(resolve => setTimeout(resolve, 2000));
    } catch (error) {
      logger.logError(`❌ Failed to subscribe to topic ${this.topic}`, error);
      throw error;
    }
  }

  _logConnectionSuccess() {
    logger.logConnectionEvent(
      this.constructor.name,
      `connected and subscribed to topic: ${this.topic} (group: ${this.consumerGroup})`
    );
  }

  async startConsuming() {
    this._ensureConsumerIsConnected();

    logger.logInfo(`Starting to consume ${this.constructor.name} messages`);
    this._startStatusReporting();

    await this.consumer.run({
      partitionsConsumedConcurrently: this.partitionsConsumedConcurrently,
      eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
        await this._handleIncomingMessage(
          topic,
          partition,
          message,
          heartbeat,
          pause
        );
      },
    });
  }

  _ensureConsumerIsConnected() {
    if (!this.isConnected) {
      throw new Error("Consumer must be connected before starting consumption");
    }
  }

  _startStatusReporting() {
    const reportInterval = this._resolveStatusReportInterval();
    this._logStatusReportingStart(reportInterval);

    this.statusInterval = setInterval(() => {
      this._reportStatusIfActive();
    }, reportInterval);
  }

  _resolveStatusReportInterval() {
    const envValue = parseInt(process.env.STATUS_REPORT_INTERVAL);
    return (
      envValue ||
      AbstractConsumer.CONFIGURATION_DEFAULTS.STATUS_REPORT_INTERVAL_MS
    );
  }

  _logStatusReportingStart(reportInterval) {
    logger.logInfo(
      `📡 Starting status reporting every ${reportInterval / 1000} seconds`
    );
  }

  _reportStatusIfActive() {
    const stats = this.processor.getStats();

    if (this._shouldShowProcessingStats(stats)) {
      this.showStats();
    }
  }

  _shouldShowProcessingStats(stats) {
    return (
      stats.totalProcessed > 0 || stats.activeTasks > 0 || stats.queueLength > 0
    );
  }

  showStats() {
    const status = this.getConcurrentStatus();
    this._logDetailedProcessingStatistics(status);
  }

  getConcurrentStatus() {
    return this.processor.getDetailedStatus();
  }

  _logDetailedProcessingStatistics(status) {
    logger.logInfo("📊 CONCURRENT PROCESSING STATUS");
    logger.logConcurrencyStatus(
      status.concurrency.active,
      status.concurrency.max,
      status.concurrency.queued
    );
    logger.logProcessingStatistics({
      processed: status.performance.processed,
      failed: status.performance.failed,
      successRate: status.performance.successRate,
      queueLength: status.queue.length,
      oldestWaitingMs: Math.round(status.queue.oldestWaitingMs),
    });
  }

  async _handleIncomingMessage(topic, partition, message, heartbeat, pause) {
    try {
      const messageData = this._parseMessageData(message);
      this._logIncomingMessage(topic, partition, message);

      await this._processMessageWithConcurrencyControl(
        messageData,
        heartbeat,
        pause
      );
    } catch (error) {
      await this._handleMessageProcessingError(error, partition, pause);
    }
  }

  _parseMessageData(message) {
    try {
      return JSON.parse(message.value.toString());
    } catch (error) {
      throw new Error(`Failed to parse message data: ${error.message}`);
    }
  }

  _logIncomingMessage(topic, partition, message) {
    logger.logDebug("Received message", {
      topic,
      partition,
      offset: message.offset,
      messageKey: message.key?.toString(),
      concurrentStatus: `${this.processor.activeTasks.size}/${this.maxConcurrency} active`,
    });
  }

  async _processMessageWithConcurrencyControl(messageData, heartbeat, pause) {
    await this.processor.processMessage(messageData, async data => {
      await heartbeat();
      return await this._processItemWithDuplicationChecks(data, pause);
    });
  }

  async _processItemWithDuplicationChecks(messageData, pause) {
    const messageId = await this.getMessageId(messageData);
    const messageKey = await this.getMessageKey(messageData);

    if (await this._isAlreadyProcessing(messageId)) {
      logger.logWarning(
        `⚠️ Item ${messageKey} is already being processed, skipping`
      );
      return;
    }

    if (await this._isAlreadyCompleted(messageId)) {
      logger.logInfo(`✅ Item ${messageKey} already completed, skipping`);
      return;
    }

    try {
      return await this._executeProcessingWithStateManagement(
        messageId,
        messageData
      );
    } catch (error) {
      if (this._shouldPauseOnBusinessError(error)) {
        this._pausePartitionForBusinessError(error, pause);
      }
      throw error;
    }
  }

  async _isAlreadyProcessing(messageId) {
    return await this.redisService.isProcessing(messageId);
  }

  async _isAlreadyCompleted(messageId) {
    return await this.isItemCompleted(messageId);
  }

  async _executeProcessingWithStateManagement(messageId, messageData) {
    try {
      await this._startProcessing(messageId);
      const result = await this._executeBusinessLogic(messageData);
      await this._handleProcessingSuccess(messageId, result);
      return result;
    } catch (error) {
      await this._handleProcessingFailure(messageId, error);
      throw error;
    } finally {
      await this._cleanupProcessing(messageId);
    }
  }

  async _startProcessing(messageId) {
    await this.redisService.setProcessingKey(messageId);
  }

  async _executeBusinessLogic(messageData) {
    return await this.process(messageData);
  }

  async _handleProcessingSuccess(messageId, result) {
    await this.markItemAsCompleted(messageId);

    if (result) {
      await this.handleProcessingResult(messageId, result);
    }

    await this.onSuccess(messageId);
  }

  async _handleProcessingFailure(messageId, error) {
    logger.logError(`Processing failed for item ${messageId}`, error);

    try {
      await this.markItemAsFailed(messageId);
      await this.onFailed(messageId, error);
    } catch (updateError) {
      logger.logError(
        "Failed to update state after processing error",
        updateError
      );
    }
  }

  async _cleanupProcessing(messageId) {
    try {
      await this.redisService.removeProcessingKey(messageId);
    } catch (error) {
      logger.logError(
        `Failed to cleanup processing key for ${messageId}`,
        error
      );
    }
  }

  _shouldPauseOnBusinessError(error) {
    return AbstractConsumer.BUSINESS_PAUSABLE_ERRORS.some(
      errorType => error.name === errorType || error.message.includes(errorType)
    );
  }

  _pausePartitionForBusinessError(error, pause) {
    logger.logWarning(
      `⏸️ Pausing partition due to business logic error: ${error.message}`
    );
    const resumePartition = pause();

    const delay = this._getErrorRecoveryDelay(error);
    setTimeout(() => {
      logger.logInfo(`▶️ Resuming partition after ${delay}ms`);
      resumePartition();
    }, delay);
  }

  _getErrorRecoveryDelay(error) {
    if (this._isRateLimitError(error)) {
      return AbstractConsumer.ERROR_RECOVERY_DELAYS.RATE_LIMIT_ERROR;
    }

    if (this._isNetworkError(error)) {
      return AbstractConsumer.ERROR_RECOVERY_DELAYS.NETWORK_ERROR;
    }

    if (this._isDatabaseError(error)) {
      return AbstractConsumer.ERROR_RECOVERY_DELAYS.DEFAULT;
    }

    return AbstractConsumer.ERROR_RECOVERY_DELAYS.BUSINESS_ERROR;
  }

  _isRateLimitError(error) {
    return (
      error.name === "RateLimitExceededError" ||
      error.message.includes("RateLimitExceeded")
    );
  }

  _isNetworkError(error) {
    const networkErrors = [
      "ServiceUnavailableError",
      "CircuitBreakerOpenError",
    ];
    return networkErrors.some(
      errorType => error.name === errorType || error.message.includes(errorType)
    );
  }

  _isDatabaseError(error) {
    return (
      error.name === "DatabaseConnectionError" ||
      error.message.includes("DatabaseConnection")
    );
  }

  async _handleMessageProcessingError(error, partition, pause) {
    logger.logError("Error handling message", error);

    if (this._shouldPausePartitionOnError(error)) {
      this._pausePartitionForErrorRecovery(error, partition, pause);
    }
  }

  _shouldPausePartitionOnError(error) {
    return AbstractConsumer.PAUSABLE_ERRORS.some(
      errorType => error.name === errorType || error.message.includes(errorType)
    );
  }

  _pausePartitionForErrorRecovery(error, partition, pause) {
    logger.logWarning(
      `⏸️ Pausing partition ${partition} due to error: ${error.message}`
    );

    const resumePartition = pause();
    const delay = AbstractConsumer.ERROR_RECOVERY_DELAYS.DEFAULT;

    setTimeout(() => {
      logger.logInfo(`▶️ Resuming partition ${partition}`);
      resumePartition();
    }, delay);
  }

  setMaxConcurrency(newMaxConcurrency) {
    this._validateMaxConcurrency(newMaxConcurrency);
    this.maxConcurrency = newMaxConcurrency;
    this.processor.setMaxConcurrency(newMaxConcurrency);
  }

  _validateMaxConcurrency(newMaxConcurrency) {
    if (typeof newMaxConcurrency !== "number" || newMaxConcurrency < 1) {
      throw new Error("Max concurrency must be a positive number");
    }
  }

  async disconnect() {
    try {
      this._stopStatusReporting();
      await this._disconnectFromAllServices();
      await this._waitForTaskCompletion();
      this.isConnected = false;
    } catch (error) {
      logger.logError("Error during disconnect", error);
    }
  }

  _stopStatusReporting() {
    if (this.statusInterval) {
      clearInterval(this.statusInterval);
      this.statusInterval = null;
    }
  }

  async _disconnectFromAllServices() {
    if (this.consumer) {
      await this.consumer.disconnect();
    }
    await this.redisService.disconnect();
  }

  async _waitForTaskCompletion() {
    logger.logInfo("⏳ Waiting for concurrent tasks to complete...");
    await this.processor.waitForCompletion();
    logger.logInfo("✅ All concurrent tasks completed");
  }

  /**
   * Abstract Methods - Must be implemented by concrete consumers
   */

  /**
   * Get unique identifier from message data
   * @param {Object} messageData - The message data object
   * @returns {Promise<string>|string} Unique identifier for the item
   */
  async getMessageId(messageData) {
    throw new Error(
      "getMessageId method must be implemented by concrete consumer"
    );
  }

  /**
   * Get display key from message data (for logging)
   * @param {Object} messageData - The message data object
   * @returns {Promise<string>|string} Display-friendly key for the item
   */
  async getMessageKey(messageData) {
    throw new Error(
      "getMessageKey method must be implemented by concrete consumer"
    );
  }

  /**
   * Process the item (business logic)
   * @param {Object} messageData - The message data to process
   * @returns {Promise<*>} Processing result
   */
  async process(messageData) {
    throw new Error("process method must be implemented by concrete consumer");
  }

  /**
   * Mark item as completed
   * @param {string} itemId - Item identifier
   */
  async markItemAsCompleted(itemId) {
    throw new Error(
      "markItemAsCompleted method must be implemented by concrete consumer"
    );
  }

  /**
   * Mark item as failed
   * @param {string} itemId - Item identifier
   */
  async markItemAsFailed(itemId) {
    throw new Error(
      "markItemAsFailed method must be implemented by concrete consumer"
    );
  }

  /**
   * Check if item is already completed
   * @param {string} itemId - Item identifier
   * @returns {Promise<boolean>} True if item is completed
   */
  async isItemCompleted(itemId) {
    throw new Error(
      "isItemCompleted method must be implemented by concrete consumer"
    );
  }

  /**
   * Hook Methods - Can be overridden by concrete consumers
   */

  /**
   * Handle processing result (save content, update additional fields, etc.)
   * @param {string} id - Item identifier
   * @param {*} result - Processing result from process() method
   */
  async handleProcessingResult(id, result) {
    logger.logInfo(`📄 Consumer: Processing result for ID: ${id}`);
  }

  /**
   * Called when processing succeeds
   * @param {string} id - Item identifier
   */
  async onSuccess(id) {
    logger.logInfo(`✅ Consumer: Successfully processed item ID: ${id}`);
  }

  /**
   * Called when processing fails
   * @param {string} id - Item identifier
   * @param {Error} error - The error that occurred
   */
  async onFailed(id, error) {
    logger.logError(`❌ Consumer: Failed to process item ID: ${id}`, error);
  }
}

module.exports = AbstractConsumer;
