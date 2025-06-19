require("dotenv").config();

class ProducerRunner {
  constructor() {
    this.scheduler = null;
    this.statusInterval = null;
    this.implementationType = this._resolveImplementationType();
  }

  static get IMPLEMENTATION_TYPES() {
    return {
      CRAWLER: "crawler",
      EMAIL: "email",
    };
  }

  static get DEFAULT_IMPLEMENTATION() {
    return ProducerRunner.IMPLEMENTATION_TYPES.CRAWLER;
  }

  static get STATUS_REPORT_INTERVAL_MS() {
    return 60000; // 1 minute
  }

  async start() {
    try {
      await this._initializeScheduler();
      this._setupGracefulShutdown();
      await this._startSchedulerWithLogging();
      this._startStatusReporting();
    } catch (error) {
      this._handleStartupError(error);
    }
  }

  _resolveImplementationType() {
    return (
      process.env.KJO_IMPLEMENTATION_TYPE ||
      ProducerRunner.DEFAULT_IMPLEMENTATION
    );
  }

  async _initializeScheduler() {
    const SchedulerClass = this._loadSchedulerClass();
    this.scheduler = new SchedulerClass();
    await this.scheduler.start();
  }

  _loadSchedulerClass() {
    const implementationType = this.implementationType.toLowerCase();

    switch (implementationType) {
      case ProducerRunner.IMPLEMENTATION_TYPES.CRAWLER:
        return require(`../examples/crawler/crawlerScheduler`);
      case ProducerRunner.IMPLEMENTATION_TYPES.EMAIL:
        this._handleEmailImplementationNotice();
        return null;
      default:
        throw new Error(`Unknown implementation type: ${implementationType}`);
    }
  }

  _handleEmailImplementationNotice() {
    console.log(
      "Email implementation doesn't have a scheduler yet - use producer directly"
    );
    process.exit(0);
  }

  _setupGracefulShutdown() {
    process.on("SIGTERM", this._createShutdownHandler("SIGTERM"));
    process.on("SIGINT", this._createShutdownHandler("SIGINT"));
  }

  _createShutdownHandler(signalName) {
    return async () => {
      console.log(`Received ${signalName}, shutting down gracefully...`);
      await this._shutdownScheduler();
      process.exit(0);
    };
  }

  async _shutdownScheduler() {
    this._stopStatusReporting();

    if (this.scheduler) {
      await this.scheduler.stop();
    }
  }

  _stopStatusReporting() {
    if (this.statusInterval) {
      clearInterval(this.statusInterval);
      this.statusInterval = null;
    }
  }

  async _startSchedulerWithLogging() {
    this._logProducerStarting();
    this._logUsageInstructions();
  }

  _logProducerStarting() {
    console.log(`\n📡 Producer (${this.implementationType}) is running...`);
  }

  _logUsageInstructions() {
    console.log("Press Ctrl+C to stop");
  }

  _startStatusReporting() {
    this.statusInterval = setInterval(() => {
      this._reportSchedulerStatus();
    }, ProducerRunner.STATUS_REPORT_INTERVAL_MS);
  }

  _reportSchedulerStatus() {
    const status = this.scheduler.getStatus();
    this._logSchedulerStatus(status);
  }

  _logSchedulerStatus(status) {
    const statusMessage = this._buildStatusMessage(status);
    console.log(statusMessage);
  }

  _buildStatusMessage(status) {
    return `\n📊 Status: Connected=${status.connected}, New Jobs=${status.newWebsitesJobRunning}, Postponed Jobs=${status.postponedWebsitesJobRunning}`;
  }

  _handleStartupError(error) {
    console.error(`Error starting ${this.implementationType} producer:`, error);
    this._displayAvailableImplementations();
    process.exit(1);
  }

  _displayAvailableImplementations() {
    console.log("\nAvailable implementations:");
    console.log("- crawler (default)");
    console.log("- email (no scheduler - use examples directly)");
    console.log(
      "\nSet KJO_IMPLEMENTATION_TYPE environment variable to choose implementation"
    );
  }
}

const producerRunner = new ProducerRunner();
producerRunner.start();
