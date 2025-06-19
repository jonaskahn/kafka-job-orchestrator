const { AbstractConsumer } = require("@jonaskahn/kafka-job-orchestrator");

// const { ObjectId } = require("mongodb");

/**
 * Example Email Consumer extending AbstractConsumer
 *
 * Demonstrates how easy it is to create new consumers with the abstract base class
 */
class EmailConsumer extends AbstractConsumer {
  constructor(topicOptions = {}) {
    const config = {
      topic: process.env.KJO_EMAIL_TOPIC || "email",
      consumerGroup: process.env.KJO_EMAIL_CONSUMER_GROUP || "email-group",
      redisOptions: {
        keyPrefix: process.env.KJO_EMAIL_REDIS_PREFIX || "EMAIL",
      },
      topicOptions,
    };

    super(config);

    // Initialize your own services
    // this.emailService = new EmailService();
    // this.smtpService = new SmtpService();
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
    // await this.smtpService.connect();
  }

  async disconnect() {
    await super.disconnect();
    // await this.emailService.disconnect();
    // await this.smtpService.disconnect();
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
    return `${messageData.to} - ${messageData.subject}`;
  }

  /**
   * Process the email (business logic)
   */
  async process(messageData) {
    const { to, subject } = messageData;

    console.log(`Sending email to: ${to}`);
    console.log(`Subject: ${subject}`);

    // Simulate email sending with random success/failure
    const success = Math.random() > 0.2; // 80% success rate

    if (!success) {
      throw new Error("SMTP server temporarily unavailable");
    }

    // Simulate processing time
    await new Promise(resolve => setTimeout(resolve, 1000));

    console.log("Email sent successfully!");

    return {
      messageId: `msg_${Date.now()}`,
      sentAt: new Date(),
      status: "sent",
    };

    // Real implementation would be:
    // return await this.smtpService.sendEmail(to, subject, body);
  }

  /**
   * Mark item as completed
   */
  async markItemAsCompleted(id) {
    console.log(`Marking email ${id} as completed`);

    // Mock implementation
    // Real implementation would be:
    // await this.emailService.updateEmailState(new ObjectId(id), EmailConsumer.STATES.COMPLETED);
  }

  /**
   * Mark item as failed
   */
  async markItemAsFailed(id) {
    console.log(`Marking email ${id} as failed`);

    // Mock implementation
    // Real implementation would be:
    // await this.emailService.updateEmailState(new ObjectId(id), EmailConsumer.STATES.FAILED);
  }

  /**
   * Check if email is already completed
   */
  async isItemCompleted(id) {
    console.log(`Checking if email ${id} is completed`);

    // Mock implementation - always return false for demo
    return false;

    // Real implementation would be:
    // const email = await this.emailService.findEmailById(new ObjectId(id));
    // return email && email.state === EmailConsumer.STATES.COMPLETED;
  }

  /**
   * Handle processing result (save delivery info, etc.)
   */
  async handleProcessingResult(id, result) {
    console.log(`Saving email delivery result for ${id}:`, result);

    // Mock implementation
    // Real implementation would be:
    // const collection = this.emailService.getCollection();
    // await collection.updateOne(
    //   { _id: new ObjectId(id) },
    //   {
    //     $set: {
    //       messageId: result.messageId,
    //       sentAt: result.sentAt,
    //       status: result.status,
    //       updatedAt: new Date(),
    //     },
    //   }
    // );
  }

  /**
   * Called when email processing succeeds
   */
  async onSuccess(id) {
    console.log(`✅ EmailConsumer: Successfully sent email ID: ${id}`);
    // Add custom email consumer success logic here
    // e.g., update analytics, send notifications, etc.
  }

  /**
   * Called when email processing fails
   */
  async onFailed(id, error) {
    console.log(
      `❌ EmailConsumer: Failed to send email ID: ${id}, Error: ${error.message}`
    );
    // Add custom email consumer failure logic here
    // e.g., log to error tracking service, alert admins, etc.
  }
}

module.exports = EmailConsumer;
