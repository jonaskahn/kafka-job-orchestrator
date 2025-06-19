const { redisClient } = require("../client/redis");
const crypto = require("crypto");
const logger = require("./loggerService");

/**
 * Redis Service for managing distributed processing state
 *
 * Handles:
 * - Processing key management (prevents duplicate processing)
 * - Sent message tracking (prevents duplicate sends)
 * - Message hash generation for deduplication
 * - Connection lifecycle management
 */
class RedisService {
  /**
   * Creates a new Redis service instance
   * @param {RedisOptions} options - Redis configuration options
   */
  constructor(options = {}) {
    this._validateOptions(options);
    this.config = this._createConfiguration(options);
    this.isConnected = false;
  }

  static get CONFIGURATION_DEFAULTS() {
    return {
      KEY_PREFIX: process.env.KJO_REDIS_KEY_PREFIX || "SUPERNOVA",
      PROCESSING_TTL_SECONDS:
        parseInt(process.env.KJO_REDIS_PROCESSINGTTL_SECONDS) || 10,
      SENT_TTL_SECONDS:
        parseInt(process.env.KJO_REDIS_SENT_TTL_SECONDS) ||
        RedisService.CONFIGURATION_DEFAULTS.PROCESSING_TTL_SECONDS * 2,
      HASH_ALGORITHM: "md5",
    };
  }

  static get KEY_SUFFIXES() {
    return {
      PROCESSING:
        process.env.KJO_REDIS_KEY_SUFFIXES_PROCESSING || "_PROCESSING:",
      SENT: process.env.KJO_REDIS_KEY_SUFFIXES_SENT || "SENT:",
    };
  }

  static get KEY_EXISTS() {
    return 1;
  }

  static get REQUIRED_ITEM_PROPERTIES() {
    return process.env.KJO_REDIS_ENTITY_REQUIRED_PROPERTIES
      ? process.env.KJO_REDIS_ENTITY_REQUIRED_PROPERTIES.split(",")
      : ["_id", "state"];
  }

  _validateOptions(options) {
    if (!options || typeof options !== "object") {
      throw new Error("Options must be a valid object");
    }

    if (!this._isNonEmptyString(options.keyPrefix)) {
      throw new Error("keyPrefix is required and must be a non-empty string");
    }
  }

  _createConfiguration(options) {
    return {
      processingPrefix: this._createKeyPattern(
        options.keyPrefix,
        RedisService.KEY_SUFFIXES.PROCESSING
      ),
      sentPrefix: this._createKeyPattern(
        options.keyPrefix,
        RedisService.KEY_SUFFIXES.SENT
      ),
      processingTtl:
        options.processingTtl ||
        RedisService.CONFIGURATION_DEFAULTS.PROCESSING_TTL_SECONDS,
      sentTtl:
        options.sentTtl || RedisService.CONFIGURATION_DEFAULTS.SENT_TTL_SECONDS,
    };
  }

  _createKeyPattern(prefix, suffix) {
    return `${prefix}${suffix}`;
  }

  /**
   * Connect to Redis
   * @returns {Promise<void>}
   */
  async connect() {
    if (this._isAlreadyConnected()) {
      this._useExistingConnection();
      return;
    }

    await this._establishConnection();
  }

  _isAlreadyConnected() {
    return redisClient.isOpen;
  }

  _useExistingConnection() {
    this.isConnected = true;
    logger.logConnectionEvent(
      "Redis",
      "✅ already connected, reusing connection"
    );
  }

  async _establishConnection() {
    try {
      await redisClient.connect();
      this.isConnected = true;
      logger.logConnectionEvent("Redis", "✅ connected successfully");
    } catch (error) {
      this.isConnected = false;
      logger.logError("❌ Redis connection error", error);
      throw error;
    }
  }

  /**
   * Disconnect from Redis
   * @returns {Promise<void>}
   */
  async disconnect() {
    try {
      await this._closeConnectionIfOpen();
    } catch (error) {
      this._handleDisconnectionError(error);
    }
  }

  async _closeConnectionIfOpen() {
    if (redisClient.isOpen) {
      await redisClient.disconnect();
    }
    this.isConnected = false;
  }

  _handleDisconnectionError(error) {
    logger.logError("❌ Redis disconnect error", error);
    this.isConnected = false;
  }

  /**
   * Set processing key for an item
   * @param {string} itemId - Item identifier
   * @returns {Promise<void>}
   */
  async setProcessingKey(itemId) {
    this._validateItemId(itemId);
    this._ensureConnected();

    const key = this._createProcessingKey(itemId);
    const value = "processing";
    const ttl = this.config.processingTtl;

    await this._setKeyWithTTL(key, value, ttl);
    this._logKeyOperation("Set processing key", key, ttl);
  }

  _createProcessingKey(itemId) {
    return this._createKey(this.config.processingPrefix, itemId);
  }

  _createKey(prefix, itemId) {
    return `${prefix}${itemId}`;
  }

  async _setKeyWithTTL(key, value, ttlSeconds) {
    await redisClient.setEx(key, ttlSeconds, value);
  }

  _logKeyOperation(operation, key, ttl = null) {
    const ttlInfo = ttl ? ` with TTL ${ttl}s` : "";
    logger.logDebug(`📝 ${operation}: ${key}${ttlInfo}`);
  }

  /**
   * Remove processing key for an item
   * @param {string} itemId - Item identifier
   * @returns {Promise<void>}
   */
  async removeProcessingKey(itemId) {
    this._validateItemId(itemId);
    this._ensureConnected();

    const key = this._createProcessingKey(itemId);
    await redisClient.del(key);
    this._logKeyOperation("Removed processing key", key);
  }

  /**
   * Check if item is currently being processed
   * @param {string} itemId - Item identifier
   * @returns {Promise<boolean>} True if processing
   */
  async isProcessing(itemId) {
    this._validateItemId(itemId);
    this._ensureConnected();

    const key = this._createProcessingKey(itemId);
    const exists = await redisClient.exists(key);
    return exists === RedisService.KEY_EXISTS;
  }

  /**
   * Get all processing item IDs
   * @returns {Promise<Array<string>>} Array of processing IDs
   */
  async getProcessingIds() {
    this._ensureConnected();
    return await this._getIdsByType("processing", this.config.processingPrefix);
  }

  async _getIdsByType(typeName, keyPrefix) {
    try {
      const keys = await this._findKeysByPattern(keyPrefix);
      const ids = this._extractIdsFromKeys(keys, keyPrefix);
      this._logFoundIds(ids, typeName);
      return ids;
    } catch (error) {
      this._logRetrievalError(error, typeName);
      return [];
    }
  }

  async _findKeysByPattern(pattern) {
    return await redisClient.keys(`${pattern}*`);
  }

  _extractIdsFromKeys(keys, prefix) {
    return keys.map(key => key.replace(prefix, ""));
  }

  _logFoundIds(ids, idType) {
    logger.logDebug(`🔍 Found ${ids.length} ${idType} IDs`, { ids });
  }

  _logRetrievalError(error, idType) {
    logger.logError(`❌ Error getting ${idType} IDs`, error);
  }

  /**
   * Set sent message tracking
   * @param {string} itemId - Item identifier
   * @param {string} messageHash - Message hash
   * @returns {Promise<void>}
   */
  async setSentMessage(itemId, messageHash) {
    this._validateItemId(itemId);
    this._validateMessageHash(messageHash);
    this._ensureConnected();

    const key = this._createSentKey(itemId);
    const ttl = this.config.sentTtl;

    await redisClient.setEx(key, ttl, messageHash);
    this._logKeyOperation("Set sent key", key, ttl);
  }

  _createSentKey(itemId) {
    return this._createKey(this.config.sentPrefix, itemId);
  }

  /**
   * Get sent message hash
   * @param {string} itemId - Item identifier
   * @returns {Promise<string|null>} Message hash or null
   */
  async getSentMessageHash(itemId) {
    this._validateItemId(itemId);
    this._ensureConnected();

    const key = this._createSentKey(itemId);
    return await redisClient.get(key);
  }

  /**
   * Check if item was sent recently with same content
   * @param {string} itemId - Item identifier
   * @param {string} messageHash - Message hash to compare
   * @returns {Promise<boolean>} True if sent recently with same hash
   */
  async isSentRecently(itemId, messageHash) {
    this._validateItemId(itemId);
    this._validateMessageHash(messageHash);

    const storedHash = await this.getSentMessageHash(itemId);
    return storedHash === messageHash;
  }

  /**
   * Get all recently sent item IDs
   * @returns {Promise<Array<string>>} Array of sent IDs
   */
  async getSentIds() {
    this._ensureConnected();
    return await this._getIdsByType("recently sent", this.config.sentPrefix);
  }

  /**
   * Generate message hash for deduplication
   * @param {Object} item - Item object
   * @returns {string} MD5 hash
   */
  generateMessageHash(item) {
    this._validateItem(item);

    const hashContent = this._createHashContent(item);
    return this._computeHash(hashContent);
  }

  _createHashContent(item) {
    const id = item._id.toString();
    const url = item.URL;
    const state = item.state;

    return `${id}-${url}-${state}`;
  }

  _computeHash(content) {
    return crypto
      .createHash(RedisService.CONFIGURATION_DEFAULTS.HASH_ALGORITHM)
      .update(content)
      .digest("hex");
  }

  // ==================== VALIDATION METHODS ====================

  _ensureConnected() {
    if (!this.isConnected) {
      throw new Error("Redis client must be connected before operations");
    }
  }

  _validateItemId(itemId) {
    if (!this._isNonEmptyString(itemId)) {
      throw new Error("ID must be a non-empty string");
    }
  }

  _validateMessageHash(messageHash) {
    if (!this._isNonEmptyString(messageHash)) {
      throw new Error("Message hash must be a non-empty string");
    }
  }

  _validateItem(item) {
    if (!this._isObject(item)) {
      throw new Error("Item must be a valid object");
    }

    const missingProperties = this._findMissingProperties(item);
    if (missingProperties.length > 0) {
      throw new Error(
        `Item must have these properties: ${missingProperties.join(", ")}`
      );
    }
  }

  _isNonEmptyString(value) {
    return value && typeof value === "string";
  }

  _isObject(value) {
    return value && typeof value === "object";
  }

  _findMissingProperties(item) {
    return RedisService.REQUIRED_ITEM_PROPERTIES.filter(
      prop => !this._hasProperty(item, prop)
    );
  }

  _hasProperty(item, property) {
    return property === "state"
      ? typeof item[property] !== "undefined"
      : Boolean(item[property]);
  }
}

module.exports = RedisService;
