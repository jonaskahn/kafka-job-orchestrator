#!/usr/bin/env node
require("dotenv").config();

const CrawlerScheduler = require("./crawlerScheduler");

async function runCrawlerProducer() {
  const scheduler = new CrawlerScheduler();

  // Handle graceful shutdown
  process.on("SIGTERM", async () => {
    console.log("Received SIGTERM, shutting down gracefully...");
    await scheduler.stop();
    process.exit(0);
  });

  process.on("SIGINT", async () => {
    console.log("Received SIGINT, shutting down gracefully...");
    await scheduler.stop();
    process.exit(0);
  });

  try {
    await scheduler.start();
    console.log("\n🕷️ Crawler Producer is running...");
    console.log("Press Ctrl+C to stop");

    // Keep the process alive with status updates
    setInterval(() => {
      const status = scheduler.getStatus();
      console.log(
        `\n📊 Crawler Status: Connected=${status.connected}, New Jobs=${status.newWebsitesJobRunning}, Postponed Jobs=${status.postponedWebsitesJobRunning}`
      );
    }, 60000);
  } catch (error) {
    console.error("Error starting crawler producer:", error);
    process.exit(1);
  }
}

// Check if this is being run directly
if (require.main === module) {
  runCrawlerProducer();
}

module.exports = { runCrawlerProducer };
