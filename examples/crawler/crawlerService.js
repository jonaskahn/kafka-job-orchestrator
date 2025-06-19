class CrawlerService {
  constructor() {
    this.processingDelay = 2000; // 2 seconds simulation
  }

  async crawlWebsite(url) {
    console.log(`Starting to crawl: ${url}`);

    // Simulate processing time
    await this.delay(this.processingDelay);

    // Simulate random success/failure (80% success rate)
    const success = Math.random() > 0.2;

    if (success) {
      // Generate fake content
      const fakeContent = this.generateFakeContent(url);
      console.log(`Successfully crawled: ${url}`);
      return {
        success: true,
        content: fakeContent,
      };
    } else {
      console.log(`Failed to crawl: ${url}`);
      throw new Error(`Failed to crawl website: ${url}`);
    }
  }

  generateFakeContent(url) {
    const titles = [
      "Welcome to our amazing website",
      "Latest news and updates",
      "Product catalog and services",
      "About our company",
      "Contact us for more information",
    ];

    const descriptions = [
      "This is a comprehensive website with lots of useful information.",
      "We provide the best services in the industry.",
      "Our products are trusted by thousands of customers.",
      "Learn more about our company history and mission.",
      "Get in touch with our expert team today.",
    ];

    const randomTitle = titles[Math.floor(Math.random() * titles.length)];
    const randomDescription =
      descriptions[Math.floor(Math.random() * descriptions.length)];

    return {
      title: randomTitle,
      description: randomDescription,
      url,
      wordCount: Math.floor(Math.random() * 1000) + 100,
      crawledAt: new Date().toISOString(),
      keywords: ["web", "content", "information", "service", "company"],
    };
  }

  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = CrawlerService;
