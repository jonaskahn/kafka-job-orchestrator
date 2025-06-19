#!/usr/bin/env node
require("dotenv").config();

const EmailProducer = require("./EmailProducer");
const EmailConsumer = require("./EmailConsumer");

async function runEmailProducer() {
  const producer = new EmailProducer();

  try {
    await producer.connect();
    console.log("\n📧 Email Producer connected!");

    // Send some emails
    console.log("Sending new emails...");
    await producer.produceNewEmails();

    console.log("Sending failed emails...");
    await producer.produceFailedEmails();

    console.log("✅ Email messages sent!");

    await producer.disconnect();
  } catch (error) {
    console.error("Error running email producer:", error);
    process.exit(1);
  }
}

async function runEmailConsumer() {
  const consumer = new EmailConsumer();

  // Handle graceful shutdown
  process.on("SIGTERM", async () => {
    console.log("Received SIGTERM, shutting down gracefully...");
    await consumer.disconnect();
    process.exit(0);
  });

  process.on("SIGINT", async () => {
    console.log("Received SIGINT, shutting down gracefully...");
    await consumer.disconnect();
    process.exit(0);
  });

  try {
    await consumer.connect();
    console.log("\n📧 Email Consumer is running...");
    console.log("Press Ctrl+C to stop");

    // Start consuming messages
    await consumer.startConsuming();
  } catch (error) {
    console.error("Error starting email consumer:", error);
    process.exit(1);
  }
}

// Command line interface
const command = process.argv[2];

switch (command) {
  case "producer":
    runEmailProducer();
    break;
  case "consumer":
    runEmailConsumer();
    break;
  default:
    console.log("📧 Email Example Runner");
    console.log("");
    console.log("Usage:");
    console.log("  node run-email.js producer  # Send email messages");
    console.log("  node run-email.js consumer  # Start email consumer");
    console.log("");
    console.log("Examples:");
    console.log("  npm run example:email:producer");
    console.log("  npm run example:email:consumer");
    process.exit(1);
}

module.exports = { runEmailProducer, runEmailConsumer };
