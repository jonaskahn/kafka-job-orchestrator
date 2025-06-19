const { AbstractProducer } = require("@jonaskahn/kafka-job-orchestrator");
const WebsiteService = require("./websiteService");

class CrawlerProducer extends AbstractProducer {
  constructor(topicOptions = {}) {
    const config = {
      topic: process.env.KJO_CRAWLER_TOPIC || "crawler",
      redisOptions: {
        keyPrefix: process.env.KJO_CRAWLER_REDIS_PREFIX || "CRAWLER",
      },
      topicOptions,
    };

    super(config);
    this.websiteService = new WebsiteService();
  }

  // State constants for this specific implementation
  // Note: PROCESSING state is virtual (Redis-only), not stored in database
  static get STATES() {
    return {
      PENDING: 1,
      POSTPONED: 2,
      COMPLETED: 3,
    };
  }

  async connect() {
    await super.connect();
    await this.websiteService.connect();
  }

  async disconnect() {
    await super.disconnect();
    await this.websiteService.disconnect();
  }

  async produceNewWebsites(limit = 100) {
    return await this.produceMessages(
      { state: CrawlerProducer.STATES.PENDING },
      limit,
      "new"
    );
  }

  async producePostponedWebsites(limit = 5) {
    return await this.produceMessages(
      { state: CrawlerProducer.STATES.POSTPONED },
      limit,
      "retry"
    );
  }

  // ==================== ABSTRACT METHOD IMPLEMENTATIONS ====================

  /**
   * Get next items to process from data source
   */
  async getNextProcessingItems(criteria, limit, excludedIds) {
    // Extract state from criteria object
    const { state } = criteria;
    return await this.websiteService.findWebsitesByState(
      state,
      limit,
      excludedIds
    );
  }

  /**
   * Get unique identifier for an item
   */
  getItemId(item) {
    return item._id.toString();
  }

  /**
   * Get display key for an item (for logging)
   */
  getItemKey(item) {
    return item.URL;
  }

  /**
   * Called when message sending succeeds
   */
  async onSuccess(id) {
    // Can add custom producer success logic here
    console.log(`✅ Producer: Successfully sent website ID: ${id}`);
  }

  /**
   * Called when message sending fails
   */
  async onFailed(id, error) {
    // Can add custom producer failure logic here
    console.log(
      `❌ Producer: Failed to send website ID: ${id}, Error: ${error.message}`
    );
  }
}

module.exports = CrawlerProducer;
