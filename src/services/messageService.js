const crypto = require("crypto");
const logger = require("./loggerService");

/**
 * Message Service for Kafka message creation and deduplication
 *
 * Handles:
 * - Stable message key generation for Kafka idempotent producer
 * - Message hash generation for Redis deduplication
 * - Kafka message creation with enhanced headers
 * - Result analysis and batch operations
 * - Debug logging for message details
 */
class MessageService {
  /**
   * Creates a new message service
   */
  constructor() {
    this.deduplicationStrategy = this._selectDeduplicationStrategy();
  }

  static get CONFIGURATION_DEFAULTS() {
    return {
      DEDUPLICATION_STRATEGY: process.env.KJO_DEDUPLICATION_STRATEGY || "redis",
      HASH_ALGORITHM: "sha256",
      HASH_LENGTH: 16,
      MD5_ALGORITHM: "md5",
    };
  }

  static get DEDUPLICATION_STRATEGIES() {
    return {
      HYBRID: "hybrid",
      REDIS: "redis",
      KAFKA: "kafka",
    };
  }

  static get MESSAGE_HEADERS() {
    return {
      MESSAGE_TYPE: "message-type",
      ITEM_ID: "item-id",
      STABLE_KEY: "stable-key",
      CONTENT_HASH: "content-hash",
      TIMESTAMP: "timestamp",
      DEDUPLICATION_STRATEGY: "deduplication-strategy",
    };
  }

  static get REQUIRED_ITEM_PROPERTIES() {
    return ["_id", "state"];
  }

  static get OPTIONAL_IDENTIFIER_FIELDS() {
    return ["URL", "email", "name"];
  }

  static get LOG_LEVELS() {
    return {
      DEBUG: "debug",
    };
  }

  _selectDeduplicationStrategy() {
    const strategyFromEnv = process.env.DEDUPLICATION_STRATEGY;
    const defaultStrategy =
      MessageService.CONFIGURATION_DEFAULTS.DEDUPLICATION_STRATEGY;

    if (!strategyFromEnv) {
      return defaultStrategy;
    }

    if (this._isValidStrategy(strategyFromEnv)) {
      return strategyFromEnv;
    }

    this._warnInvalidStrategy(strategyFromEnv, defaultStrategy);
    return defaultStrategy;
  }

  _isValidStrategy(strategy) {
    const validStrategies = Object.values(
      MessageService.DEDUPLICATION_STRATEGIES
    );
    return validStrategies.includes(strategy);
  }

  _warnInvalidStrategy(invalidStrategy, defaultStrategy) {
    logger.logWarning(
      `⚠️ Invalid deduplication strategy: ${invalidStrategy}. Using default: ${defaultStrategy}`
    );
  }

  /**
   * Generate stable message key for Kafka idempotent producer
   * Works with any data item that has been processed by concrete producer
   * @param {Object} itemData - Item data object
   * @param {string} messageType - Type of message
   * @returns {string} Stable message key
   */
  generateStableMessageKey(itemData, messageType) {
    this._validateItemData(itemData);
    this._validateMessageType(messageType);

    const keyContent = this._buildStableKeyContent(itemData, messageType);
    const hash = this._hashContent(keyContent);
    const truncatedHash = this._truncateHash(hash);

    return this._formatStableKey(messageType, truncatedHash);
  }

  _buildStableKeyContent(itemData, messageType) {
    const baseContent = this._buildItemIdentifier(itemData);
    return `${baseContent}-${messageType}`;
  }

  _buildItemIdentifier(itemData) {
    const id = itemData._id.toString();
    const state = itemData.state;
    const additionalId = this._findAdditionalIdentifier(itemData);

    return `${id}${additionalId}-${state}`;
  }

  _findAdditionalIdentifier(itemData) {
    const identifierField = MessageService.OPTIONAL_IDENTIFIER_FIELDS.find(
      field => itemData[field]
    );

    return identifierField ? `-${itemData[identifierField]}` : "";
  }

  _hashContent(content) {
    return crypto
      .createHash(MessageService.CONFIGURATION_DEFAULTS.HASH_ALGORITHM)
      .update(content)
      .digest("hex");
  }

  _truncateHash(hash) {
    return hash.substring(0, MessageService.CONFIGURATION_DEFAULTS.HASH_LENGTH);
  }

  _formatStableKey(messageType, truncatedHash) {
    return `${messageType}-${truncatedHash}`;
  }

  /**
   * Generate message hash for Redis deduplication
   * @param {Object} itemData - Item data object
   * @returns {string} MD5 hash for Redis deduplication
   */
  generateRedisHash(itemData) {
    this._validateItemData(itemData);

    const hashContent = this._buildRedisHashContent(itemData);
    return this._hashContentMD5(hashContent);
  }

  _buildRedisHashContent(itemData) {
    return this._buildItemIdentifier(itemData);
  }

  _hashContentMD5(content) {
    return crypto
      .createHash(MessageService.CONFIGURATION_DEFAULTS.MD5_ALGORITHM)
      .update(content)
      .digest("hex");
  }

  /**
   * Create Kafka message with enhanced headers for traceability
   * Follows official KafkaJS message structure documentation
   * @param {Object} itemData - Item data object
   * @param {string} messageType - Type of message
   * @param {string} stableKey - Stable message key
   * @returns {Object} Kafka message object
   */
  createKafkaMessage(itemData, messageType, stableKey) {
    this._validateItemData(itemData);
    this._validateMessageType(messageType);
    this._validateStableKey(stableKey);

    return {
      key: stableKey,
      value: this._serializeMessageValue(itemData, messageType),
      headers: this._buildMessageHeaders(itemData, messageType, stableKey),
    };
  }

  _serializeMessageValue(itemData, messageType) {
    const messageContent = {
      id: itemData._id.toString(),
      data: itemData,
      type: messageType,
    };

    return JSON.stringify(messageContent);
  }

  _buildMessageHeaders(itemData, messageType, stableKey) {
    const headers = MessageService.MESSAGE_HEADERS;

    return {
      [headers.MESSAGE_TYPE]: messageType,
      [headers.ITEM_ID]: itemData._id.toString(),
      [headers.STABLE_KEY]: stableKey,
      [headers.CONTENT_HASH]: this.generateRedisHash(itemData),
      [headers.TIMESTAMP]: Date.now().toString(),
      [headers.DEDUPLICATION_STRATEGY]: this.deduplicationStrategy,
    };
  }

  /**
   * Create batch of Kafka messages for multiple items
   * @param {Array} itemsWithHashes - Array of items with their hashes
   * @param {string} messageType - Type of message
   * @returns {Array} Array of Kafka messages
   */
  createKafkaMessages(itemsWithHashes, messageType) {
    this._validateItemsArray(itemsWithHashes);
    this._validateMessageType(messageType);

    return itemsWithHashes.map(({ data }) =>
      this._createSingleMessage(data, messageType)
    );
  }

  _createSingleMessage(data, messageType) {
    const stableKey = this.generateStableMessageKey(data, messageType);
    return this.createKafkaMessage(data, messageType, stableKey);
  }

  /**
   * Generate unique identifier for message batch
   * @returns {string} Batch ID
   */
  generateBatchId() {
    const BATCH_ID_BYTES = 8;
    return crypto.randomBytes(BATCH_ID_BYTES).toString("hex");
  }

  /**
   * Analyze message sending result
   * @param {Object} result - Kafka send result
   * @param {number} messageCount - Number of messages sent
   * @returns {Object} Analysis summary
   */
  analyzeResult(result, messageCount) {
    this._validateResultAnalysisInputs(result, messageCount);

    const summary = this._createBaseSummary(messageCount);
    this._addBrokerResponseIfAvailable(summary, result);

    return summary;
  }

  _createBaseSummary(messageCount) {
    return {
      totalMessages: messageCount,
      successful: messageCount,
      failed: 0,
      duplicatesRejected: 0,
    };
  }

  _addBrokerResponseIfAvailable(summary, result) {
    if (this._hasBrokerResponse(result)) {
      summary.brokerResponse = result[0];
    }
  }

  _hasBrokerResponse(result) {
    return result && result.length > 0;
  }

  /**
   * Log detailed message information for debugging
   * @param {Array} messages - Array of Kafka messages
   * @param {string} batchId - Batch identifier
   */
  logMessageDetails(messages, batchId) {
    this._validateLogInputs(messages, batchId);

    if (!this._isDebugMode()) {
      return;
    }

    this._logBatchHeader(batchId);
    messages.forEach((message, index) =>
      this._logSingleMessage(message, index + 1)
    );
  }

  _isDebugMode() {
    return process.env.LOG_LEVEL === MessageService.LOG_LEVELS.DEBUG;
  }

  _logBatchHeader(batchId) {
    logger.logBatchOperation(batchId, "🔍 Message Details");
  }

  _logSingleMessage(message, displayIndex) {
    const messageValue = JSON.parse(message.value);

    logger.logDebug(`Message ${displayIndex}`, {
      key: message.key,
      data: messageValue.data,
      headers: message.headers,
    });
  }

  // ==================== VALIDATION METHODS ====================

  _validateItemData(itemData) {
    if (!this._isObject(itemData)) {
      throw new Error("Item data must be a valid object");
    }

    const missingProperties = this._findMissingRequiredProperties(itemData);
    if (missingProperties.length > 0) {
      throw new Error(
        `Item data must have these properties: ${missingProperties.join(", ")}`
      );
    }
  }

  _isObject(value) {
    return value && typeof value === "object";
  }

  _findMissingRequiredProperties(itemData) {
    return MessageService.REQUIRED_ITEM_PROPERTIES.filter(
      prop => !this._hasRequiredProperty(itemData, prop)
    );
  }

  _hasRequiredProperty(item, property) {
    return property === "state"
      ? typeof item[property] !== "undefined"
      : Boolean(item[property]);
  }

  _validateMessageType(messageType) {
    if (!this._isNonEmptyString(messageType)) {
      throw new Error("Message type must be a non-empty string");
    }
  }

  _validateStableKey(stableKey) {
    if (!this._isNonEmptyString(stableKey)) {
      throw new Error("Stable key must be a non-empty string");
    }
  }

  _validateItemsArray(items) {
    if (!Array.isArray(items)) {
      throw new Error("Items must be an array");
    }

    if (items.length === 0) {
      throw new Error("Items array cannot be empty");
    }
  }

  _validateResultAnalysisInputs(result, messageCount) {
    if (!this._isNonNegativeNumber(messageCount)) {
      throw new Error("Message count must be a non-negative number");
    }
  }

  _validateLogInputs(messages, batchId) {
    if (!Array.isArray(messages)) {
      throw new Error("Messages must be an array");
    }

    if (!this._isNonEmptyString(batchId)) {
      throw new Error("Batch ID must be a non-empty string");
    }
  }

  _isNonEmptyString(value) {
    return value && typeof value === "string";
  }

  _isNonNegativeNumber(value) {
    return typeof value === "number" && value >= 0;
  }
}

module.exports = MessageService;
