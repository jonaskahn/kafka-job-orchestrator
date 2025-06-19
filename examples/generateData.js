require("dotenv").config();
const WebsiteService = require("./crawler/websiteService");

async function generateSampleData() {
  const websiteService = new WebsiteService();

  try {
    await websiteService.connect();
    console.log("Connected to MongoDB");

    // Sample URLs to crawl
    const sampleUrls = [
      "https://example.com",
      "https://google.com",
      "https://github.com",
      "https://stackoverflow.com",
      "https://news.ycombinator.com",
      "https://reddit.com",
      "https://medium.com",
      "https://dev.to",
      "https://linkedin.com",
      "https://twitter.com",
      "https://facebook.com",
      "https://youtube.com",
      "https://amazon.com",
      "https://wikipedia.org",
      "https://techcrunch.com",
      "https://bbc.com",
      "https://cnn.com",
      "https://1npmjs.com",
      "https://1expressjs.com",
      "https://1nodejs.org",
      "https://1example.com",
      "https://1google.com",
      "https://1github.com",
      "https://1stackoverflow.com",
      "https://1news.ycombinator.com",
      "https://1reddit.com",
      "https://1medium.com",
      "https://1dev.to",
      "https://1linkedin.com",
      "https://1twitter.com",
      "https://1facebook.com",
      "https://1youtube.com",
      "https://1amazon.com",
      "https://1wikipedia.org",
      "https://1techcrunch.com",
      "https://1bbc.com",
      "https://1cnn.com",
      "https://1npmjs.com",
      "https://1expressjs.com",
      "https://1nodejs.org",

      "https://1npmjs.com1",
      "https://1expressjs.com1",
      "https://1nodejs.org1",
      "https://1example.com1",
      "https://1google.com1",
      "https://1github.com1",
      "https://1stackoverflow.com1",
      "https://1news.ycombinator.com1",
      "https://1reddit.com1",
      "https://1medium.com1",
      "https://1dev.t1o",
      "https://1linkedin.com1",
      "https://1twitter.com1",
      "https://1facebook.com1",
      "https://1youtube.com1",
      "https://1amazon.com1",
      "https://1wikipedia.org1",
      "https://1techcrunch.com1",
      "https://1bbc.com1",
      "https://1cnn.com1",
      "https://1npmjs.com1",
      "https://1expressjs.com1",
      "https://1nodejs.org1",
    ];

    // Clear existing data
    const collection = websiteService.getCollection();
    await collection.deleteMany({});
    console.log("Cleared existing website data");

    // Insert new sample data
    let insertedCount = 0;

    // Insert 15 websites with state=1 (new)
    for (let i = 0; i < sampleUrls.length; i++) {
      const url = sampleUrls[i % sampleUrls.length];
      const uniqueUrl = i < sampleUrls.length ? url : `${url}/${i}`;
      await websiteService.createWebsite(uniqueUrl, null, 1);
      insertedCount++;
    }

    console.log(`✅ Successfully inserted ${insertedCount} sample websites:`);
    console.log("   - 15 websites with state=1 (new)");
    console.log("   - 10 websites with state=2 (postponed)");

    // Show summary
    const stateCount1 = await collection.countDocuments({ state: 1 });
    const stateCount2 = await collection.countDocuments({ state: 2 });
    const total = await collection.countDocuments({});

    console.log("\n📊 Database Summary:");
    console.log(`   Total websites: ${total}`);
    console.log(`   State 1 (new): ${stateCount1}`);
    console.log(`   State 2 (postponed): ${stateCount2}`);
  } catch (error) {
    console.error("Error generating sample data:", error);
  } finally {
    await websiteService.disconnect();
    console.log("Disconnected from MongoDB");
    process.exit(0);
  }
}

// Run if this script is executed directly
if (require.main === module) {
  generateSampleData();
}

module.exports = generateSampleData;
