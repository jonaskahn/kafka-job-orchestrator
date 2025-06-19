require("dotenv").config();

class ConsumerRunner {
  constructor() {
    this.consumer = null;
    this.implementationType = this._resolveImplementationType();
  }

  static get IMPLEMENTATION_TYPES() {
    return {
      CRAWLER: "crawler",
      EMAIL: "email",
    };
  }

  static get DEFAULT_IMPLEMENTATION() {
    return ConsumerRunner.IMPLEMENTATION_TYPES.CRAWLER;
  }

  async start() {
    try {
      await this._initializeConsumer();
      this._setupGracefulShutdown();
      await this._startConsumerWithLogging();
    } catch (error) {
      this._handleStartupError(error);
    }
  }

  _resolveImplementationType() {
    return (
      process.env.KJO_IMPLEMENTATION_TYPE ||
      ConsumerRunner.DEFAULT_IMPLEMENTATION
    );
  }

  async _initializeConsumer() {
    const ConsumerClass = this._loadConsumerClass();
    this.consumer = new ConsumerClass();
    await this.consumer.connect();
  }

  _loadConsumerClass() {
    const implementationType = this.implementationType.toLowerCase();

    switch (implementationType) {
      case ConsumerRunner.IMPLEMENTATION_TYPES.CRAWLER:
        return require(`./crawler/crawlerConsumer`);
      case ConsumerRunner.IMPLEMENTATION_TYPES.EMAIL:
        return require(`./email/EmailConsumer`);
      default:
        throw new Error(`Unknown implementation type: ${implementationType}`);
    }
  }

  _setupGracefulShutdown() {
    process.on("SIGTERM", this._createShutdownHandler("SIGTERM"));
    process.on("SIGINT", this._createShutdownHandler("SIGINT"));
  }

  _createShutdownHandler(signalName) {
    return async () => {
      console.log(`Received ${signalName}, shutting down gracefully...`);
      await this._shutdownConsumer();
      process.exit(0);
    };
  }

  async _shutdownConsumer() {
    if (this.consumer) {
      await this.consumer.disconnect();
    }
  }

  async _startConsumerWithLogging() {
    this._logConsumerStarting();
    this._logUsageInstructions();
    await this.consumer.startConsuming();
  }

  _logConsumerStarting() {
    console.log(`\n🎯 Consumer (${this.implementationType}) is running...`);
  }

  _logUsageInstructions() {
    console.log("Press Ctrl+C to stop");
  }

  _handleStartupError(error) {
    console.error(`Error starting ${this.implementationType} consumer:`, error);
    this._displayAvailableImplementations();
    process.exit(1);
  }

  _displayAvailableImplementations() {
    console.log("\nAvailable implementations:");
    console.log("- crawler (default)");
    console.log("- email");
    console.log(
      "\nSet KJO_IMPLEMENTATION_TYPE environment variable to choose implementation"
    );
  }
}

const consumerRunner = new ConsumerRunner();
consumerRunner.start();
