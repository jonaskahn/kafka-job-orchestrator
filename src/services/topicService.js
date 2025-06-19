const { kafkaClient } = require("../client/kafka");
const logger = require("./loggerService");

/**
 * Topic Service for managing Kafka topics with partition configuration
 *
 * Handles:
 * - Topic creation and partition management
 * - Use case specific configurations
 * - Topic metadata and listing
 * - Environment and constructor parameter support
 * - Automatic partition scaling
 */
class TopicService {
  /**
   * Creates a new topic service
   * @param {Object} options - Configuration options
   */
  constructor(options = {}) {
    this.config = this._createServiceConfiguration(options);
    this._logConfiguration();
  }

  static get CONFIGURATION_DEFAULTS() {
    return {
      PARTITIONS: parseInt(process.env.KJO_KAFKA_TOPIC_PARTITIONS) || 4,
      REPLICATION_FACTOR:
        parseInt(process.env.KJO_KAFKA_TOPIC_REPLICATION_FACTOR) || 1,
      AUTO_CREATE_TOPICS:
        process.env.KJO_KAFKA_TOPIC_AUTO_CREATE_TOPICS === "true",
      RETENTION_MS: process.env.KJO_KAFKA_TOPIC_RETENTION_MS || "604800000", // 7 days
      SEGMENT_MS: process.env.KJO_KAFKA_TOPIC_SEGMENT_MS || "86400000", // 1 day
      COMPRESSION_TYPE:
        process.env.KJO_KAFKA_TOPIC_COMPRESSION_TYPE || "producer",
      CLEANUP_POLICY: process.env.KJO_KAFKA_TOPIC_CLEANUP_POLICY || "delete",
    };
  }

  static get TIME_CONSTANTS() {
    return {
      ONE_HOUR_MS: 3600000,
      ONE_DAY_MS: 86400000,
      SEVEN_DAYS_MS: 604800000,
      TEN_MINUTES_MS: 600000,
    };
  }

  static get USE_CASE_CONFIGURATIONS() {
    return {
      "high-throughput": {
        partitions: 16,
        config: {
          "retention.ms": TopicService.TIME_CONSTANTS.ONE_DAY_MS.toString(),
          "segment.ms": TopicService.TIME_CONSTANTS.ONE_HOUR_MS.toString(),
          "compression.type": "lz4",
        },
      },
      "low-latency": {
        partitions: 8,
        config: {
          "batch.size": "1",
          "linger.ms": "0",
          "compression.type": "none",
        },
      },
      development: {
        partitions: 2,
        config: {
          "retention.ms": TopicService.TIME_CONSTANTS.ONE_HOUR_MS.toString(),
          "segment.ms": TopicService.TIME_CONSTANTS.TEN_MINUTES_MS.toString(),
        },
      },
      production: {
        partitions: TopicService.CONFIGURATION_DEFAULTS.PARTITIONS,
        config: {
          "retention.ms": TopicService.TIME_CONSTANTS.SEVEN_DAYS_MS.toString(),
          "segment.ms": TopicService.TIME_CONSTANTS.ONE_DAY_MS.toString(),
        },
      },
    };
  }

  static get SYSTEM_TOPIC_PREFIX() {
    return "__";
  }

  _createServiceConfiguration(options) {
    return {
      partitions: this._resolvePartitions(options),
      replicationFactor: this._resolveReplicationFactor(options),
      autoCreate: this._resolveAutoCreate(options),
      defaultConfig: this._createDefaultTopicConfig(options),
    };
  }

  _resolvePartitions(options) {
    return this._resolveConfigValue(
      options.partitions,
      process.env.KJO_KAFKA_TOPIC_PARTITIONS,
      TopicService.CONFIGURATION_DEFAULTS.PARTITIONS,
      parseInt
    );
  }

  _resolveReplicationFactor(options) {
    return this._resolveConfigValue(
      options.replicationFactor,
      process.env.KJO_KAFKA_REPLICATION_FACTOR,
      TopicService.CONFIGURATION_DEFAULTS.REPLICATION_FACTOR,
      parseInt
    );
  }

  _resolveAutoCreate(options) {
    if (options.autoCreate !== undefined) {
      return options.autoCreate;
    }

    return (
      process.env.KAFKA_AUTO_CREATE_TOPICS === "true" ||
      TopicService.CONFIGURATION_DEFAULTS.AUTO_CREATE_TOPICS
    );
  }

  _resolveConfigValue(optionValue, envValue, defaultValue, parser = null) {
    if (optionValue !== undefined) {
      return optionValue;
    }

    if (envValue) {
      return parser ? parser(envValue) : envValue;
    }

    return defaultValue;
  }

  _createDefaultTopicConfig(options) {
    const defaults = TopicService.CONFIGURATION_DEFAULTS;

    return {
      "retention.ms": this._resolveConfigValue(
        options.retentionMs,
        process.env.TOPIC_RETENTION_MS,
        defaults.RETENTION_MS
      ),
      "segment.ms": this._resolveConfigValue(
        options.segmentMs,
        process.env.TOPIC_SEGMENT_MS,
        defaults.SEGMENT_MS
      ),
      "compression.type": options.compressionType || defaults.COMPRESSION_TYPE,
      "cleanup.policy": options.cleanupPolicy || defaults.CLEANUP_POLICY,
    };
  }

  _logConfiguration() {
    logger.logInfo("🎛️ TopicService configured", {
      defaultPartitions: this.config.partitions,
      replicationFactor: this.config.replicationFactor,
      autoCreateTopics: this.config.autoCreate,
    });
  }

  /**
   * Ensure topic exists with specified partition count
   * @param {string} topicName - Name of the topic
   * @param {number|null} partitionCount - Number of partitions (optional)
   * @param {Object} topicConfig - Additional topic configuration (optional)
   * @returns {Promise<Object>} Topic creation/verification result
   */
  async ensureTopicExists(topicName, partitionCount = null, topicConfig = {}) {
    this._validateTopicName(topicName);

    const adminClient = kafkaClient.admin();
    try {
      await adminClient.connect();
      return await this._ensureTopicWithClient(
        adminClient,
        topicName,
        partitionCount,
        topicConfig
      );
    } finally {
      await adminClient.disconnect();
    }
  }

  async _ensureTopicWithClient(
    adminClient,
    topicName,
    partitionCount,
    topicConfig
  ) {
    const existingTopic = await this._findTopic(adminClient, topicName);

    if (existingTopic) {
      return await this._handleExistingTopic(
        adminClient,
        topicName,
        existingTopic,
        partitionCount
      );
    }

    return await this._createNewTopic(
      adminClient,
      topicName,
      partitionCount,
      topicConfig
    );
  }

  async _findTopic(adminClient, topicName) {
    const metadata = await adminClient.fetchTopicMetadata({
      topics: [topicName],
    });

    const topic = metadata.topics.find(t => t.name === topicName);
    return this._isValidTopic(topic) ? topic : null;
  }

  _isValidTopic(topic) {
    return topic && !topic.errorCode;
  }

  async _handleExistingTopic(
    adminClient,
    topicName,
    existingTopic,
    requestedPartitions
  ) {
    const currentPartitions = existingTopic.partitions.length;
    const desiredPartitions = requestedPartitions || this.config.partitions;

    this._logExistingTopic(topicName, currentPartitions);

    if (this._shouldIncreasePartitions(currentPartitions, desiredPartitions)) {
      await this._increasePartitions(adminClient, topicName, desiredPartitions);
      return this._createResult(false, desiredPartitions, true);
    }

    return this._createResult(false, currentPartitions, true);
  }

  _shouldIncreasePartitions(current, desired) {
    return current < desired;
  }

  async _increasePartitions(adminClient, topicName, newPartitionCount) {
    this._logPartitionIncrease(topicName, newPartitionCount);

    await adminClient.createPartitions({
      topicPartitions: [
        {
          topic: topicName,
          count: newPartitionCount,
        },
      ],
    });

    this._logPartitionIncreaseSuccess(topicName, newPartitionCount);
  }

  async _createNewTopic(adminClient, topicName, partitionCount, topicConfig) {
    this._ensureAutoCreateEnabled();

    const partitions = partitionCount || this.config.partitions;
    const mergedConfig = { ...this.config.defaultConfig, ...topicConfig };

    await this._createTopicWithConfig(
      adminClient,
      topicName,
      partitions,
      mergedConfig
    );

    return this._createResult(true, partitions, false);
  }

  _ensureAutoCreateEnabled() {
    if (!this.config.autoCreate) {
      throw new Error("Topic does not exist and auto-creation is disabled");
    }
  }

  async _createTopicWithConfig(adminClient, topicName, partitions, config) {
    this._logTopicCreation(topicName, partitions);

    await adminClient.createTopics({
      topics: [
        {
          topic: topicName,
          numPartitions: partitions,
          replicationFactor: this.config.replicationFactor,
          configEntries: this._formatConfigEntries(config),
        },
      ],
    });

    this._logTopicCreationSuccess(topicName, partitions);
  }

  _formatConfigEntries(config) {
    return Object.entries(config).map(([name, value]) => ({
      name,
      value: String(value),
    }));
  }

  _createResult(created, partitions, existed) {
    return {
      created,
      partitions,
      existed,
    };
  }

  /**
   * Get topic partition count
   * @param {string} topicName - Name of the topic
   * @returns {Promise<number>} Number of partitions
   */
  async getTopicPartitionCount(topicName) {
    this._validateTopicName(topicName);

    const adminClient = kafkaClient.admin();
    try {
      await adminClient.connect();
      return await this._fetchPartitionCount(adminClient, topicName);
    } finally {
      await adminClient.disconnect();
    }
  }

  async _fetchPartitionCount(adminClient, topicName) {
    const metadata = await adminClient.fetchTopicMetadata({
      topics: [topicName],
    });

    const topic = metadata.topics.find(t => t.name === topicName);

    if (!this._isValidTopic(topic)) {
      throw new Error(`Topic '${topicName}' not found`);
    }

    return topic.partitions.length;
  }

  /**
   * Configure topic for specific use case
   * @param {string} topicName - Name of the topic
   * @param {string} useCase - Use case name
   * @returns {Promise<Object>} Topic configuration result
   */
  async configureTopicForUseCase(topicName, useCase) {
    this._validateTopicName(topicName);
    this._validateUseCase(useCase);

    const configuration = this._getUseCaseConfig(useCase);
    this._logUseCaseApplication(topicName, useCase);

    return await this.ensureTopicExists(
      topicName,
      configuration.partitions,
      configuration.config
    );
  }

  _getUseCaseConfig(useCase) {
    const configuration = TopicService.USE_CASE_CONFIGURATIONS[useCase];

    // Production use case should use configured partitions
    if (useCase === "production") {
      return {
        ...configuration,
        partitions: this.config.partitions,
      };
    }

    return configuration;
  }

  _logUseCaseApplication(topicName, useCase) {
    logger.logInfo(
      `🎯 Configuring topic '${topicName}' for use case: ${useCase}`
    );
  }

  /**
   * List all topics with partition information
   * @returns {Promise<Array>} Array of topic information
   */
  async listTopicsWithPartitions() {
    const adminClient = kafkaClient.admin();
    try {
      await adminClient.connect();
      return await this._fetchAllTopics(adminClient);
    } finally {
      await adminClient.disconnect();
    }
  }

  async _fetchAllTopics(adminClient) {
    const topicNames = await adminClient.listTopics();
    const metadata = await adminClient.fetchTopicMetadata({
      topics: topicNames,
    });

    return metadata.topics.filter(this._isUserTopic).map(this._mapTopicInfo);
  }

  _isUserTopic(topic) {
    return !topic.name.startsWith(TopicService.SYSTEM_TOPIC_PREFIX);
  }

  _mapTopicInfo(topic) {
    return {
      name: topic.name,
      partitions: topic.partitions.length,
      replicas: topic.partitions[0]?.replicas.length || 0,
    };
  }

  // ==================== VALIDATION METHODS ====================

  _validateTopicName(topicName) {
    if (!this._isNonEmptyString(topicName)) {
      throw new Error("Topic name must be a non-empty string");
    }
  }

  _validateUseCase(useCase) {
    const availableUseCases = Object.keys(TopicService.USE_CASE_CONFIGURATIONS);

    if (!availableUseCases.includes(useCase)) {
      throw new Error(
        `Unknown use case: ${useCase}. Available: ${availableUseCases.join(
          ", "
        )}`
      );
    }
  }

  _isNonEmptyString(value) {
    return value && typeof value === "string";
  }

  // ==================== LOGGING METHODS ====================

  _logExistingTopic(topicName, partitionCount) {
    logger.logInfo(
      `📊 Topic '${topicName}' exists with ${partitionCount} partitions`
    );
  }

  _logPartitionIncrease(topicName, newPartitionCount) {
    logger.logInfo(
      `📈 Increasing partitions to ${newPartitionCount} for topic '${topicName}'`
    );
  }

  _logPartitionIncreaseSuccess(topicName, partitionCount) {
    logger.logInfo(
      `✅ Topic '${topicName}' now has ${partitionCount} partitions`
    );
  }

  _logTopicCreation(topicName, partitions) {
    logger.logInfo(
      `🆕 Creating topic '${topicName}' with ${partitions} partitions`
    );
  }

  _logTopicCreationSuccess(topicName, partitions) {
    logger.logInfo(
      `✅ Topic '${topicName}' created with ${partitions} partitions`
    );
  }
}

module.exports = TopicService;
