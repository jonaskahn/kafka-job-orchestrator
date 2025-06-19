const { mongoClient } = require("../mongodb");
const { ObjectId } = require("mongodb");

class WebsiteService {
  constructor() {
    this.dbName = process.env.KJO_MONGODB_DATABASE || "crawler_db";
    this.collectionName = "website";
  }

  async connect() {
    try {
      await mongoClient.connect();
      console.log("MongoDB connected successfully");
    } catch (error) {
      console.error("MongoDB connection error:", error);
      throw error;
    }
  }

  async disconnect() {
    await mongoClient.close();
  }

  getCollection() {
    return mongoClient.db(this.dbName).collection(this.collectionName);
  }

  async findWebsitesByState(state, limit, excludeIds = []) {
    const collection = this.getCollection();

    // Convert string IDs to ObjectIds
    const objectIds = excludeIds.map(id =>
      typeof id === "string" ? new ObjectId(id) : id
    );

    const query = {
      state,
      ...(objectIds.length > 0 && { _id: { $nin: objectIds } }),
    };

    return await collection.find(query).limit(limit).toArray();
  }

  async findWebsiteById(id) {
    const collection = this.getCollection();
    return await collection.findOne({ _id: id });
  }

  async updateWebsiteState(id, newState) {
    const collection = this.getCollection();
    return await collection.updateOne(
      { _id: id },
      { $set: { state: newState } }
    );
  }

  async createWebsite(url, content = null, state = 1) {
    const collection = this.getCollection();
    const website = {
      URL: url,
      content,
      state,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    return await collection.insertOne(website);
  }
}

module.exports = WebsiteService;
