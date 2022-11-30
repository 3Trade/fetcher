import { MongoClient } from "mongodb";

const url = "mongodb://mongo:27017";
const mongo_client = new MongoClient(url);

export class DBWorker {
  private db;
  private dbName;
  constructor(dbName) {
    console.log("Starting MongoDB worker...");

    this.dbName = dbName;
    this.connect();
  }

  async connect() {
    // Use connect method to connect to the server
    await mongo_client.connect();
    console.log("Mongo DB connected!!");
    this.db = mongo_client.db(this.dbName);
    // collection = db.collection('1d');

    // the following code examples can be pasted here...
    return "done.";
  }

  async saveKLines(timeframe, pair, klines) {
    const filter = { _id: pair };
    const updateDoc = {
      $set: {
        _id: pair,
        klines
      }
    };
    const result = await this.db
      .collection(timeframe)
      .updateOne(filter, updateDoc, { upsert: true });

    if (result) console.log("INSERTED: ", timeframe, pair, result);
    return result;
  }
}
