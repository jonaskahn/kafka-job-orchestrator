const { MongoClient } = require("mongodb");

class MongoDBClientConfig {
  static get CONNECTION_DEFAULTS() {
    return {
      URI: process.env.KJO_MONGO_URI || "mongodb://localhost:27017/defaultdb",
      CONNECTION_TIMEOUT_MS:
        parseInt(process.env.KJO_MONGO_CONNECTION_TIMEOUT_MS) || 10000,
      SERVER_SELECTION_TIMEOUT_MS:
        parseInt(process.env.KJO_MONGO_SERVER_SELECTION_TIMEOUT_MS) || 5000,
      RETRY_WRITE: process.env.KJO_MONGO_RETRY_WRITE === "true",
      RETRY_READ: process.env.KJO_MONGO_RETRY_READ === "true",
    };
  }

  static get POOL_DEFAULTS() {
    return {
      MAX_SIZE: parseInt(process.env.KJO_MONGO_POOL_MAX_SIZE) || 10,
      MIN_SIZE: parseInt(process.env.KJO_MONGO_POOL_MIN_SIZE) || 1,
    };
  }

  static createMongoClient() {
    this._validateConnectionUri(this.CONNECTION_DEFAULTS.URI);
    const clientOptions = this._buildClientOptions();

    const client = new MongoClient(this.CONNECTION_DEFAULTS.URI, clientOptions);
    this._addConnectionEventHandlers(client);
    return client;
  }

  static _validateConnectionUri(connectionUri) {
    this._ensureUriIsNonEmptyString(connectionUri);
    this._ensureUriHasValidProtocol(connectionUri);
  }

  static _ensureUriIsNonEmptyString(connectionUri) {
    if (!connectionUri || typeof connectionUri !== "string") {
      throw new Error("MongoDB URI must be a non-empty string");
    }
  }

  static _ensureUriHasValidProtocol(connectionUri) {
    const hasValidProtocol =
      connectionUri.startsWith("mongodb://") ||
      connectionUri.startsWith("mongodb+srv://");

    if (!hasValidProtocol) {
      throw new Error(
        "MongoDB URI must start with mongodb:// or mongodb+srv://"
      );
    }
  }

  static _buildClientOptions() {
    return {
      connectTimeoutMS: this.CONNECTION_DEFAULTS.CONNECTION_TIMEOUT_MS,
      serverSelectionTimeoutMS:
        this.CONNECTION_DEFAULTS.SERVER_SELECTION_TIMEOUT_MS,
      maxPoolSize: this.POOL_DEFAULTS.MAX_SIZE,
      minPoolSize: this.POOL_DEFAULTS.MIN_SIZE,
      retryWrites: this.CONNECTION_DEFAULTS.RETRY_WRITE,
      retryReads: this.CONNECTION_DEFAULTS.RETRY_READ,
    };
  }

  static _addConnectionEventHandlers(client) {
    client.on("open", this._handleConnectionOpen);
    client.on("close", this._handleConnectionClose);
    client.on("error", this._handleConnectionError);
    client.on("serverOpening", this._handleServerOpening);
    client.on("serverClosed", this._handleServerClosed);
  }

  static _handleConnectionOpen() {
    console.log("MongoDB client connected");
  }

  static _handleConnectionClose() {
    console.log("MongoDB client disconnected");
  }

  static _handleConnectionError(error) {
    console.error("MongoDB client error:", error);
  }

  static _handleServerOpening() {
    console.log("MongoDB server connection opening");
  }

  static _handleServerClosed() {
    console.log("MongoDB server connection closed");
  }

  static _logUsingDefaultUri() {
    console.warn("MONGODB_URI not set, using default URI");
  }
}

const mongoClient = MongoDBClientConfig.createMongoClient();

module.exports = {
  mongoClient,
};
