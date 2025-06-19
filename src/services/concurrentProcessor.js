const logger = require("./loggerService");

/**
 * Concurrent Message Processor
 *
 * Implements RabbitMQ-like prefetch mechanism for Kafka message processing
 * Controls how many messages can be processed concurrently
 *
 * Features:
 * - Task queue management with capacity control
 * - Performance statistics tracking
 * - Dynamic concurrency adjustment
 * - Detailed status reporting
 */
class ConcurrentProcessor {
  /**
   * Creates a new concurrent processor
   * @param {number} maxConcurrency - Maximum number of concurrent tasks
   */
  constructor(
    maxConcurrency = ConcurrentProcessor.CONFIGURATION_DEFAULTS.MAX_CONCURRENCY
  ) {
    this._validateMaxConcurrency(maxConcurrency);
    this._initializeProcessor(maxConcurrency);
  }

  static get CONFIGURATION_DEFAULTS() {
    return {
      MAX_CONCURRENCY: parseInt(process.env.KJO_CONCURRENT_MAX_EXECUTOR) || 5,
      POLLING_INTERVAL_MS:
        parseInt(process.env.KJO_CONCURRENT_POLLING_INTERVAL_MS) || 100,
      TASK_ID_LENGTH: parseInt(process.env.KJO_CONCURRENT_TASK_ID_LENGTH) || 9,
    };
  }

  static get TASK_STATUS() {
    return {
      PENDING: "pending",
      ACTIVE: "active",
      COMPLETED: "completed",
      FAILED: "failed",
    };
  }

  static get SUCCESS_RATE_PRECISION() {
    return parseInt(process.env.KJO_CONCURRENT_SUCCESS_RATE_PRECISION) || 2;
  }

  _validateMaxConcurrency(maxConcurrency) {
    if (typeof maxConcurrency !== "number" || maxConcurrency < 1) {
      throw new Error("Max concurrency must be a positive number");
    }
  }

  _initializeProcessor(maxConcurrency) {
    this.maxConcurrency = maxConcurrency;
    this.activeTasks = new Set();
    this.queue = [];
    this.stats = this._createInitialStatistics();
  }

  _createInitialStatistics() {
    return {
      processed: 0,
      failed: 0,
      concurrent: 0,
      maxConcurrentReached: 0,
    };
  }

  /**
   * Process a message with concurrency control
   * @param {Object} messageData - The message data to process
   * @param {Function} callbackExecutor - Function to process the message
   * @returns {Promise} Promise that resolves with processing result
   */
  async processMessage(messageData, callbackExecutor) {
    this._validateProcessingInputs(messageData, callbackExecutor);

    return new Promise((resolve, reject) => {
      const task = this._createProcessingTask(
        messageData,
        callbackExecutor,
        resolve,
        reject
      );
      this._scheduleTaskForExecution(task);
    });
  }

  _validateProcessingInputs(messageData, callbackExecutor) {
    if (!messageData) {
      throw new Error("Message data is required");
    }
    if (typeof callbackExecutor !== "function") {
      throw new Error("Processing function must be a function");
    }
  }

  _createProcessingTask(messageData, callbackExecutor, resolve, reject) {
    return {
      messageData,
      callbackExecutor,
      resolve,
      reject,
      createdAt: Date.now(),
      status: ConcurrentProcessor.TASK_STATUS.PENDING,
    };
  }

  _scheduleTaskForExecution(task) {
    if (this._hasAvailableCapacity()) {
      this._executeTaskImmediately(task);
    } else {
      this._queueTaskForLater(task);
    }
  }

  _hasAvailableCapacity() {
    return this.activeTasks.size < this.maxConcurrency;
  }

  _executeTaskImmediately(task) {
    setImmediate(() => this._executeTask(task));
  }

  _queueTaskForLater(task) {
    this.queue.push(task);
    this._logTaskQueued();
  }

  _logTaskQueued() {
    logger.logInfo(
      `⏳ Message queued (${this.queue.length} waiting, ${this.activeTasks.size}/${this.maxConcurrency} active)`
    );
  }

  async _executeTask(task) {
    const taskId = this._generateUniqueTaskId();

    try {
      this._startTaskExecution(task, taskId);
      const result = await this._runTaskFunction(task, taskId);
      this._handleTaskSuccess(task, taskId, result);
    } catch (error) {
      this._handleTaskFailure(task, taskId, error);
    } finally {
      this._finalizeTaskExecution(taskId);
    }
  }

  _generateUniqueTaskId() {
    const timestamp = Date.now();
    const randomSuffix = this._generateRandomSuffix();
    return `task-${timestamp}-${randomSuffix}`;
  }

  _generateRandomSuffix() {
    return Math.random()
      .toString(36)
      .substr(2, ConcurrentProcessor.CONFIGURATION_DEFAULTS.TASK_ID_LENGTH);
  }

  _startTaskExecution(task, taskId) {
    this._addActiveTask(taskId);
    this._markTaskAsActive(task);
    this._updateConcurrencyStatistics();
    this._logTaskStart(taskId);
  }

  _addActiveTask(taskId) {
    this.activeTasks.add(taskId);
  }

  _markTaskAsActive(task) {
    task.status = ConcurrentProcessor.TASK_STATUS.ACTIVE;
  }

  _updateConcurrencyStatistics() {
    this.stats.concurrent = this.activeTasks.size;
    this._updateMaxConcurrentReached();
  }

  _updateMaxConcurrentReached() {
    if (this.stats.concurrent > this.stats.maxConcurrentReached) {
      this.stats.maxConcurrentReached = this.stats.concurrent;
    }
  }

  _logTaskStart(taskId) {
    logger.logTaskOperation(
      taskId,
      `🔄 Processing message (${this.activeTasks.size}/${this.maxConcurrency} active)`
    );
  }

  async _runTaskFunction(task, _taskId) {
    return await task.processingFunction(task.messageData);
  }

  _handleTaskSuccess(task, taskId, result) {
    this._incrementProcessedCount();
    this._markTaskAsCompleted(task);
    this._resolveTask(task, result);
    this._logTaskSuccess(taskId);
  }

  _incrementProcessedCount() {
    this.stats.processed++;
  }

  _markTaskAsCompleted(task) {
    task.status = ConcurrentProcessor.TASK_STATUS.COMPLETED;
  }

  _resolveTask(task, result) {
    task.resolve(result);
  }

  _logTaskSuccess(taskId) {
    logger.logTaskOperation(taskId, "✅ Message processed successfully");
  }

  _handleTaskFailure(task, taskId, error) {
    this._incrementFailedCount();
    this._markTaskAsFailed(task);
    this._rejectTask(task, error);
    this._logTaskFailure(taskId, error);
  }

  _incrementFailedCount() {
    this.stats.failed++;
  }

  _markTaskAsFailed(task) {
    task.status = ConcurrentProcessor.TASK_STATUS.FAILED;
  }

  _rejectTask(task, error) {
    task.reject(error);
  }

  _logTaskFailure(taskId, error) {
    logger.logError(`❌ [${taskId}] Message processing failed`, error);
  }

  _finalizeTaskExecution(taskId) {
    this._removeActiveTask(taskId);
    this._updateConcurrencyStatistics();
    this._processNextQueuedTask();
  }

  _removeActiveTask(taskId) {
    this.activeTasks.delete(taskId);
  }

  _processNextQueuedTask() {
    if (this._canProcessNextTask()) {
      const nextTask = this._getNextQueuedTask();
      this._logProcessingNextTask();
      this._executeTaskImmediately(nextTask);
    }
  }

  _canProcessNextTask() {
    return this._hasQueuedTasks() && this._hasAvailableCapacity();
  }

  _hasQueuedTasks() {
    return this.queue.length > 0;
  }

  _getNextQueuedTask() {
    return this.queue.shift();
  }

  _logProcessingNextTask() {
    logger.logInfo(
      `📤 Processing next queued message (${this.queue.length} remaining in queue)`
    );
  }

  /**
   * Get processing statistics
   * @returns {Object} Basic statistics object
   */
  getStats() {
    return {
      ...this.stats,
      queueLength: this.queue.length,
      activeTasks: this.activeTasks.size,
      totalProcessed: this._calculateTotalProcessedCount(),
    };
  }

  _calculateTotalProcessedCount() {
    return this.stats.processed + this.stats.failed;
  }

  /**
   * Get detailed processing status
   * @returns {Object} Detailed status with concurrency, performance, and queue info
   */
  getDetailedStatus() {
    return {
      concurrency: this._getConcurrencyStatus(),
      performance: this._getPerformanceStatus(),
      queue: this._getQueueStatus(),
    };
  }

  _getConcurrencyStatus() {
    return {
      max: this.maxConcurrency,
      active: this.activeTasks.size,
      queued: this.queue.length,
      available: this._calculateAvailableCapacity(),
    };
  }

  _calculateAvailableCapacity() {
    return this.maxConcurrency - this.activeTasks.size;
  }

  _getPerformanceStatus() {
    const totalTasks = this._calculateTotalProcessedCount();
    const successRate = this._calculateSuccessRate(totalTasks);

    return {
      processed: this.stats.processed,
      failed: this.stats.failed,
      successRate,
      maxConcurrentReached: this.stats.maxConcurrentReached,
    };
  }

  _calculateSuccessRate(totalTasks) {
    if (totalTasks === 0) {
      return "0%";
    }

    const rate = (this.stats.processed / totalTasks) * 100;
    return `${rate.toFixed(ConcurrentProcessor.SUCCESS_RATE_PRECISION)}%`;
  }

  _getQueueStatus() {
    return {
      length: this.queue.length,
      oldestWaitingMs: this._calculateOldestWaitTime(),
    };
  }

  _calculateOldestWaitTime() {
    if (this.queue.length === 0) {
      return 0;
    }
    return Date.now() - this.queue[0].createdAt;
  }

  /**
   * Check if processor is at capacity
   * @returns {boolean} True if at maximum capacity
   */
  isAtCapacity() {
    return this.activeTasks.size >= this.maxConcurrency;
  }

  /**
   * Wait for all tasks to complete
   * @returns {Promise<void>} Promise that resolves when all tasks are done
   */
  async waitForCompletion() {
    while (this._hasTasksInProgress()) {
      await this._waitBriefly();
    }
  }

  _hasTasksInProgress() {
    return this._hasActiveTasks() || this._hasQueuedTasks();
  }

  _hasActiveTasks() {
    return this.activeTasks.size > 0;
  }

  async _waitBriefly() {
    return new Promise(resolve =>
      setTimeout(
        resolve,
        ConcurrentProcessor.CONFIGURATION_DEFAULTS.POLLING_INTERVAL_MS
      )
    );
  }

  /**
   * Update maximum concurrency dynamically
   * @param {number} newMaxConcurrency - New maximum concurrency value
   */
  setMaxConcurrency(newMaxConcurrency) {
    this._validateMaxConcurrency(newMaxConcurrency);
    this._logConcurrencyUpdate(newMaxConcurrency);
    this._updateMaxConcurrency(newMaxConcurrency);
    this._processQueuedTasksIfCapacityAvailable();
  }

  _logConcurrencyUpdate(newMaxConcurrency) {
    console.log(
      `🔧 Updating max concurrency from ${this.maxConcurrency} to ${newMaxConcurrency}`
    );
  }

  _updateMaxConcurrency(newMaxConcurrency) {
    this.maxConcurrency = newMaxConcurrency;
  }

  _processQueuedTasksIfCapacityAvailable() {
    while (this._canProcessNextTask()) {
      const nextTask = this._getNextQueuedTask();
      this._executeTaskImmediately(nextTask);
    }
  }
}

module.exports = ConcurrentProcessor;
