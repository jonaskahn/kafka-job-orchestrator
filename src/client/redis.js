const redis = require("redis");
const logger = require("../services/loggerService");

/**
 * Redis Client Factory
 *
 * Provides Redis client creation with:
 * - Connection configuration from environment variables
 * - Retry and reconnection strategies
 * - Event handling and logging
 * - Singleton client instance
 */
class RedisClientFactory {
  static get CONNECTION_DEFAULTS() {
    return {
      URL: process.env.KJO_REDIS_URL || "redis://localhost:6379",
      PASSWORD: process.env.KJO_REDIS_PASSWORD,
      RETRY_DELAY_MS: parseInt(process.env.KJO_REDIS_DELAY_MS) || 1000,
      MAX_RETRY_ATTEMPTS:
        parseInt(process.env.KJO_REDIS_MAX_RETRY_ATTEMPTS) || 5,
      MAX_RECONNECT_DELAY_MS:
        parseInt(process.env.KJO_REDIS_MAX_DELAY_MS) || 30000,
    };
  }

  /**
   * Creates and configures a Redis client instance
   * @returns {redis.RedisClient} Configured Redis client
   */
  static createClient() {
    const clientConfiguration = this._buildClientConfiguration();
    const client = redis.createClient(clientConfiguration);

    this._attachEventHandlers(client);

    return client;
  }

  static _buildClientConfiguration() {
    return {
      url: this.CONNECTION_DEFAULTS.URL,
      password: this.CONNECTION_DEFAULTS.PASSWORD,
      retry_strategy: this._createRetryStrategy(),
      socket: {
        reconnectStrategy: this._createReconnectionStrategy(),
      },
    };
  }

  static _createRetryStrategy() {
    return attemptNumber => {
      if (this._hasExceededMaxRetryAttempts(attemptNumber)) {
        this._logRetryLimitExceeded(attemptNumber);
        return null;
      }

      const delayMs = this._calculateRetryDelay(attemptNumber);
      this._logRetryAttempt(attemptNumber, delayMs);
      return delayMs;
    };
  }

  static _createReconnectionStrategy() {
    return retryAttemptNumber => {
      if (this._hasExceededMaxRetryAttempts(retryAttemptNumber)) {
        this._logReconnectionLimitExceeded(retryAttemptNumber);
        return new Error("Redis connection failed permanently");
      }

      const delayMs = this._calculateRetryDelay(retryAttemptNumber);
      this._logReconnectionAttempt(retryAttemptNumber, delayMs);
      return delayMs;
    };
  }

  static _hasExceededMaxRetryAttempts(attemptNumber) {
    return attemptNumber > this.CONNECTION_DEFAULTS.MAX_RETRY_ATTEMPTS;
  }

  static _calculateRetryDelay(attemptNumber) {
    const exponentialDelay =
      attemptNumber * this.CONNECTION_DEFAULTS.RETRY_DELAY_MS;
    return Math.min(
      exponentialDelay,
      this.CONNECTION_DEFAULTS.MAX_RECONNECT_DELAY_MS
    );
  }

  static _logRetryAttempt(attemptNumber, delayMs) {
    logger.logInfo(`📡 Redis retry attempt ${attemptNumber} in ${delayMs}ms`);
  }

  static _logRetryLimitExceeded(attemptNumber) {
    logger.logError(
      `❌ Redis retry limit exceeded (${attemptNumber} attempts)`
    );
  }

  static _logReconnectionAttempt(retryAttemptNumber, delayMs) {
    logger.logInfo(
      `🔄 Redis reconnecting in ${delayMs}ms (attempt ${retryAttemptNumber})`
    );
  }

  static _logReconnectionLimitExceeded(retryAttemptNumber) {
    logger.logError(
      `❌ Redis reconnection limit exceeded (${retryAttemptNumber} attempts)`
    );
  }

  static _attachEventHandlers(client) {
    client.on("error", error => {
      logger.logError("❌ Redis client error", error);
    });

    client.on("connect", () => {
      logger.logConnectionEvent("Redis", "🔌 client connected");
    });

    client.on("ready", () => {
      logger.logConnectionEvent("Redis", "✅ client ready");
    });

    client.on("end", () => {
      logger.logConnectionEvent("Redis", "🔚 client disconnected");
    });

    client.on("reconnecting", () => {
      logger.logConnectionEvent("Redis", "🔄 client reconnecting");
    });
  }
}

// Create singleton Redis client instance
const redisClient = RedisClientFactory.createClient();

module.exports = {
  redisClient,
  RedisClientFactory,
};
