const { AbstractConsumer } = require("@jonaskahn/kafka-job-orchestrator");
const WebsiteService = require("./websiteService");
const CrawlerService = require("./crawlerService");
const { ObjectId } = require("mongodb");

class CrawlerConsumer extends AbstractConsumer {
  constructor(topicOptions = {}) {
    const config = {
      topic: process.env.KJO_CRAWLER_TOPIC || "crawler",
      consumerGroup: process.env.KJO_CRAWLER_CONSUMER_GROUP || "crawler-group",
      redisOptions: {
        keyPrefix: process.env.KJO_CRAWLER_REDIS_PREFIX || "CRAWLER",
      },
      topicOptions,
    };

    super(config);
    this.websiteService = new WebsiteService();
    this.crawlerService = new CrawlerService();
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

  // ==================== ABSTRACT METHOD IMPLEMENTATIONS ====================

  /**
   * Get unique identifier from message data
   */
  getMessageId(messageData) {
    return messageData.id;
  }

  /**
   * Get display key from message data (for logging)
   */
  getMessageKey(messageData) {
    return messageData.data.URL;
  }

  /**
   * Process the item (business logic)
   */
  async process(messageData) {
    const { URL } = messageData.data;
    console.log("Starting crawler...");
    return await this.crawlerService.crawlWebsite(URL);
  }

  /**
   * Mark item as completed
   */
  async markItemAsCompleted(id) {
    await this.websiteService.updateWebsiteState(
      new ObjectId(id),
      CrawlerConsumer.STATES.COMPLETED
    );
  }

  /**
   * Mark item as failed
   */
  async markItemAsFailed(id) {
    await this.websiteService.updateWebsiteState(
      new ObjectId(id),
      CrawlerConsumer.STATES.PENDING
    );
  }

  /**
   * Check if item is already completed
   */
  async isItemCompleted(id) {
    const website = await this.websiteService.findWebsiteById(new ObjectId(id));
    return website && website.state === CrawlerConsumer.STATES.COMPLETED;
  }

  /**
   * Handle processing result (save content, update additional fields, etc.)
   */
  async handleProcessingResult(id, result) {
    const collection = this.websiteService.getCollection();
    await collection.updateOne(
      { _id: new ObjectId(id) },
      {
        $set: {
          content: result.content,
          updatedAt: new Date(),
        },
      }
    );
  }

  /**
   * Called when processing succeeds
   */
  async onSuccess(id) {
    console.log(`✅ Consumer: Successfully processed website ID: ${id}`);
  }

  /**
   * Called when processing fails
   */
  async onFailed(id, error) {
    console.log(
      `❌ Consumer: Failed to process website ID: ${id}, Error: ${error.message}`
    );
  }
}

module.exports = CrawlerConsumer;
