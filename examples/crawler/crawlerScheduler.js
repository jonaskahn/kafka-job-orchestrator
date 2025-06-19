const cron = require("node-cron");
const CrawlerProducer = require("./crawlerProducer");

class CrawlerScheduler {
  constructor() {
    this.producer = new CrawlerProducer();
    this.newWebsitesJob = null;
    this.postponedWebsitesJob = null;
    this.isConnected = false;
  }

  async start() {
    try {
      console.log("🚀 Starting Crawler Scheduler...");

      // Connect to all services
      await this.producer.connect();
      this.isConnected = true;

      console.log("✅ Crawler Scheduler connected to all services");

      // Schedule producer for new websites (state=1) - every 30 seconds
      console.log("📅 Scheduling NEW websites producer (every 30 seconds)");
      this.newWebsitesJob = cron.schedule(
        "*/30 * * * * *",
        async () => {
          if (this.isConnected) {
            try {
              await this.producer.produceNewWebsites(10);
            } catch (error) {
              console.error("Error in new websites producer job:", error);
            }
          }
        },
        {
          scheduled: false,
        }
      );

      // Schedule producer for postponed websites (state=2) - every 60 seconds
      console.log(
        "📅 Scheduling POSTPONED websites producer (every 60 seconds)"
      );
      this.postponedWebsitesJob = cron.schedule(
        "*/60 * * * * *",
        async () => {
          if (this.isConnected) {
            try {
              await this.producer.producePostponedWebsites(5);
            } catch (error) {
              console.error("Error in postponed websites producer job:", error);
            }
          }
        },
        {
          scheduled: false,
        }
      );

      // Start the cron jobs
      this.newWebsitesJob.start();
      this.postponedWebsitesJob.start();

      console.log("✅ All scheduler jobs started successfully!");
      console.log("   - New websites: every 30 seconds (limit: 10)");
      console.log("   - Postponed websites: every 60 seconds (limit: 5)");

      // Run producers immediately on startup
      console.log("\n🔄 Running initial production...");
      await this.producer.produceNewWebsites(10);
      setTimeout(async () => {
        await this.producer.producePostponedWebsites(5);
      }, 5000); // 5 second delay
    } catch (error) {
      console.error("Error starting crawler scheduler:", error);
      throw error;
    }
  }

  async stop() {
    console.log("🛑 Stopping Crawler Scheduler...");

    // Stop cron jobs
    if (this.newWebsitesJob) {
      this.newWebsitesJob.stop();
      console.log("Stopped new websites job");
    }

    if (this.postponedWebsitesJob) {
      this.postponedWebsitesJob.stop();
      console.log("Stopped postponed websites job");
    }

    // Disconnect from services
    if (this.isConnected) {
      await this.producer.disconnect();
      this.isConnected = false;
      console.log("Disconnected from all services");
    }

    console.log("✅ Crawler Scheduler stopped successfully");
  }

  getStatus() {
    return {
      connected: this.isConnected,
      newWebsitesJobRunning: this.newWebsitesJob
        ? this.newWebsitesJob.running
        : false,
      postponedWebsitesJobRunning: this.postponedWebsitesJob
        ? this.postponedWebsitesJob.running
        : false,
    };
  }
}

module.exports = CrawlerScheduler;
