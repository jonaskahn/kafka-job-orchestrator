const { AbstractProducer } = require("@jonaskahn/kafka-job-orchestrator");

// const { ObjectId } = require("mongodb");

/**
 * Example Email Producer extending AbstractProducer
 *
 * Demonstrates how easy it is to create new producers with the abstract base class
 */
class EmailProducer extends AbstractProducer {
  constructor(topicOptions = {}) {
    const config = {
      topic: process.env.KJO_EMAIL_TOPIC || "email",
      redisOptions: {
        keyPrefix: process.env.KJO_EMAIL_REDIS_PREFIX || "EMAIL",
      },
      topicOptions,
    };

    super(config);

    // Initialize your own data service
    // this.emailService = new EmailService();
  }

  // State constants for this specific implementation
  // Note: PROCESSING state is virtual (Redis-only), not stored in database
  static get STATES() {
    return {
      PENDING: 1,
      FAILED: 2,
      COMPLETED: 3,
    };
  }

  async connect() {
    await super.connect();
    // await this.emailService.connect();
  }

  async disconnect() {
    await super.disconnect();
    // await this.emailService.disconnect();
  }

  /**
   * Produce new emails to be sent
   */
  async produceNewEmails(limit = 50) {
    return await this.produceMessages(
      { state: EmailProducer.STATES.PENDING },
      limit,
      "new"
    );
  }

  /**
   * Produce failed emails to be retried
   */
  async produceFailedEmails(limit = 20) {
    return await this.produceMessages(
      { state: EmailProducer.STATES.FAILED },
      limit,
      "retry"
    );
  }

  // ==================== ABSTRACT METHOD IMPLEMENTATIONS ====================

  /**
   * Get next emails to process from data source
   */
  async getNextProcessingItems(criteria, limit, excludedIds) {
    // Extract state from criteria object
    const { state } = criteria;

    // Example implementation - replace with your own email service
    console.log(
      `Getting emails with state=${state}, limit=${limit}, excluding IDs:`,
      excludedIds
    );

    // Mock data for demonstration
    const mockEmails = [];
    for (let i = 0; i < Math.min(limit, 5); i++) {
      const id = `email_${Date.now()}_${i}`;
      if (!excludedIds.includes(id)) {
        mockEmails.push({
          _id: { toString: () => id },
          URL: `email:${id}`, // MessageService expects URL property
          to: `user${i}@example.com`,
          subject: `Test Email ${i}`,
          body: `This is test email ${i}`,
          state,
        });
      }
    }

    return mockEmails;

    // Real implementation would be:
    // return await this.emailService.findEmailsByState(state, limit, excludedIds);
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
    return `${item.to} - ${item.subject}`;
  }

  /**
   * Called when message sending succeeds
   */
  async onSuccess(id) {
    console.log(`✅ EmailProducer: Successfully sent email ID: ${id}`);
    // Add custom email producer success logic here
  }

  /**
   * Called when message sending fails
   */
  async onFailed(id, error) {
    console.log(
      `❌ EmailProducer: Failed to send email ID: ${id}, Error: ${error.message}`
    );
    // Add custom email producer failure logic here
  }
}

module.exports = EmailProducer;
