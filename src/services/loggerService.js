const winston = require("winston");

// Constants for log levels and formats
const LOG_LEVELS = {
  ERROR: "error",
  WARN: "warn",
  INFO: "info",
  DEBUG: "debug",
};

const LOG_FORMATS = {
  SIMPLE: "simple",
  JSON: "json",
  DETAILED: "detailed",
};

/**
 * Centralized logging service using Winston
 * Follows Single Responsibility Principle - handles all logging concerns
 */
class LoggerService {
  constructor() {
    this.logger = this._createLogger();
  }

  _createLogger() {
    const logLevel = this._getLogLevel();
    const logFormat = this._getLogFormat();

    return winston.createLogger({
      level: logLevel,
      format: this._createFormat(logFormat),
      transports: this._createTransports(),
      exitOnError: false,
    });
  }

  _getLogLevel() {
    return process.env.LOG_LEVEL || LOG_LEVELS.INFO;
  }

  _getLogFormat() {
    return process.env.LOG_FORMAT || LOG_FORMATS.SIMPLE;
  }

  _createFormat(formatType) {
    const timestamp = winston.format.timestamp({
      format: "YYYY-MM-DD HH:mm:ss",
    });

    const formats = {
      [LOG_FORMATS.SIMPLE]: winston.format.combine(
        timestamp,
        winston.format.colorize(),
        winston.format.printf(({ timestamp, level, message, ...meta }) => {
          const metaString = Object.keys(meta).length
            ? ` ${JSON.stringify(meta)}`
            : "";
          return `${timestamp} [${level}]: ${message}${metaString}`;
        })
      ),
      [LOG_FORMATS.JSON]: winston.format.combine(
        timestamp,
        winston.format.json()
      ),
      [LOG_FORMATS.DETAILED]: winston.format.combine(
        timestamp,
        winston.format.colorize(),
        winston.format.errors({ stack: true }),
        winston.format.printf(
          ({ timestamp, level, message, stack, ...meta }) => {
            const metaString = Object.keys(meta).length
              ? `\nMeta: ${JSON.stringify(meta, null, 2)}`
              : "";
            const stackString = stack ? `\nStack: ${stack}` : "";
            return `${timestamp} [${level}]: ${message}${metaString}${stackString}`;
          }
        )
      ),
    };

    return formats[formatType] || formats[LOG_FORMATS.SIMPLE];
  }

  _createTransports() {
    return [
      new winston.transports.Console({
        handleExceptions: true,
        handleRejections: true,
      }),
    ];
  }

  // Public logging methods following clean naming conventions
  logInfo(message, metadata = {}) {
    this.logger.info(message, metadata);
  }

  logDebug(message, metadata = {}) {
    this.logger.debug(message, metadata);
  }

  logWarning(message, metadata = {}) {
    this.logger.warn(message, metadata);
  }

  logError(message, error = null, metadata = {}) {
    const errorInfo = error
      ? {
          error: error.message,
          stack: error.stack,
        }
      : {};

    this.logger.error(message, { ...errorInfo, ...metadata });
  }

  // Specialized logging methods for common use cases
  logBatchOperation(batchId, operation, details = {}) {
    this.logInfo(`[Batch ${batchId}] ${operation}`, details);
  }

  logTaskOperation(taskId, operation, details = {}) {
    this.logInfo(`[${taskId}] ${operation}`, details);
  }

  logConnectionEvent(service, event, details = {}) {
    this.logInfo(`${service} ${event}`, details);
  }

  logConcurrencyStatus(active, max, queued, additionalInfo = {}) {
    this.logInfo(
      `Concurrency Status: ${active}/${max} active, ${queued} queued`,
      additionalInfo
    );
  }

  logProcessingStatistics(statistics) {
    this.logInfo("Processing Statistics", statistics);
  }

  // Method to check if debug logging is enabled
  isDebugEnabled() {
    return this.logger.isDebugEnabled();
  }

  // Method to check if specific level is enabled
  isLevelEnabled(level) {
    return this.logger.isLevelEnabled(level);
  }
}

// Export singleton instance to maintain consistency
module.exports = new LoggerService();
