const { Kafka, Partitioners } = require("kafkajs");
const logger = require("../services/loggerService");

/**
 * Kafka Client Factory
 *
 * Provides Kafka client and producer creation with:
 * - Connection configuration from environment variables
 * - Producer configuration with idempotent and retry settings
 * - Message sending options
 * - Singleton instances for client and producer
 */
class KafkaClientFactory {
  static get CONNECTION_DEFAULTS() {
    return {
      CLIENT_ID:
        process.env.KJO_KAFKA_CLIENT_ID ??
        `KJO_KAFKA_CLIENT.${new Date().getTime()}`,
      BROKERS: process.env.KJO_KAFKA_BROKERS
        ? process.env.KJO_KAFKA_BROKERS.split(",")
        : ["localhost:9092"],
      INITIAL_RETRY_TIME_MS:
        parseInt(process.env.KJO_KAFKA_INITIAL_RETRY_TIME_MS) || 100,
      RETRY_COUNT: parseInt(process.env.KJO_KAFKA_RETRY_COUNT) || 8,
    };
  }

  static get PRODUCER_DEFAULTS() {
    return {
      // Idempotent mode requires acks=-1 (all). Setting to false by default to allow flexible acks (0, 1, -1)
      IDEMPOTENT: process.env.KJO_KAFKA_IDEMPOTENT === "true",
      MAX_IN_FLIGHT_REQUESTS:
        parseInt(process.env.KJO_KAFKA_MAX_IN_FLIGHT_REQUESTS) || 5,
      TRANSACTION_TIMEOUT_MS:
        parseInt(process.env.KJO_KAFKA_TRANSACTION_TIMEOUT_MS) || 60000,
      METADATA_MAX_AGE_MS:
        parseInt(process.env.KJO_KAFKA_METADATA_MAX_AGE_MS) || 300000,
      ALLOW_AUTO_TOPIC_CREATION:
        process.env.KJO_KAFKA_ALLOW_AUTO_TOPIC_CREATION === "true",
    };
  }

  static get MESSAGE_SEND_DEFAULTS() {
    return {
      // Note: When IDEMPOTENT is true, acks must be -1
      ACKNOWLEDGMENT_LEVEL:
        parseInt(process.env.KJO_KAFKA_MESSAGE_ACKNOWLEDGMENT_LEVEL) || -1,
      TIMEOUT_MS: parseInt(process.env.KJO_KAFKA_MESSAGE_TIMEOUT) || 30000,
      COMPRESSION: process.env.KJO_KAFKA_MESSAGE_COMPRESSION,
    };
  }

  /**
   * Creates a Kafka client instance
   * @returns {Kafka} Configured Kafka client
   */
  static createClient() {
    const clientConfig = this._buildClientConfiguration();
    const client = new Kafka(clientConfig);

    this._logClientCreation(clientConfig);

    return client;
  }

  static _buildClientConfiguration() {
    return {
      clientId: this.CONNECTION_DEFAULTS.CLIENT_ID,
      brokers: this.CONNECTION_DEFAULTS.BROKERS,
      retry: this._createRetryConfiguration(),
    };
  }

  static _createRetryConfiguration() {
    return {
      initialRetryTime: this.CONNECTION_DEFAULTS.INITIAL_RETRY_TIME_MS,
      retries: this.CONNECTION_DEFAULTS.RETRY_COUNT,
    };
  }

  static _logClientCreation(config) {
    logger.logInfo(
      `📡 Kafka client created: ${config.clientId} connecting to ${config.brokers.join(", ")}`
    );
  }

  /**
   * Creates a Kafka producer instance
   * @param {Kafka} kafkaClient - The Kafka client instance
   * @returns {Producer} Configured Kafka producer
   */
  static createProducer(kafkaClient) {
    const producerConfig = this._buildProducerConfiguration();
    const producer = kafkaClient.producer(producerConfig);

    this._logProducerCreation(producerConfig);

    return producer;
  }

  static _buildProducerConfiguration() {
    return {
      idempotent: this.PRODUCER_DEFAULTS.IDEMPOTENT,
      maxInFlightRequests: this.PRODUCER_DEFAULTS.MAX_IN_FLIGHT_REQUESTS,
      transactionTimeout: this.PRODUCER_DEFAULTS.TRANSACTION_TIMEOUT_MS,
      metadataMaxAge: this.PRODUCER_DEFAULTS.METADATA_MAX_AGE_MS,
      allowAutoTopicCreation: this.PRODUCER_DEFAULTS.ALLOW_AUTO_TOPIC_CREATION,
      retry: this._createRetryConfiguration(),
      createPartitioner: Partitioners.LegacyPartitioner,
    };
  }

  static _logProducerCreation(config) {
    logger.logInfo(
      `✅ Kafka producer created with idempotent=${config.idempotent}, maxInFlight=${config.maxInFlightRequests}`
    );
  }

  /**
   * Gets default options for sending messages
   * @returns {Object} Message send options
   */
  static getSendOptions() {
    return {
      acks: this.MESSAGE_SEND_DEFAULTS.ACKNOWLEDGMENT_LEVEL,
      timeout: this.MESSAGE_SEND_DEFAULTS.TIMEOUT_MS,
      compression: this.MESSAGE_SEND_DEFAULTS.COMPRESSION,
    };
  }

  /**
   * Validates producer configuration consistency
   * Logs warnings if configuration might cause issues
   */
  static validateProducerConfiguration() {
    if (
      this.PRODUCER_DEFAULTS.IDEMPOTENT &&
      this.MESSAGE_SEND_DEFAULTS.ACKNOWLEDGMENT_LEVEL !== -1
    ) {
      logger.logWarning(
        `⚠️ Idempotent producer requires acks=-1, but current acks=${this.MESSAGE_SEND_DEFAULTS.ACKNOWLEDGMENT_LEVEL}`
      );
    }
  }
}

// Create singleton instances
const kafkaClient = KafkaClientFactory.createClient();
const kafkaProducer = KafkaClientFactory.createProducer(kafkaClient);

// Validate configuration on startup
KafkaClientFactory.validateProducerConfiguration();

module.exports = {
  kafkaClient,
  kafkaProducer,
  KafkaClientFactory,
};
