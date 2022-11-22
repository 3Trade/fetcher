import amqp from "amqplib";
import { delay } from "../../helpers/delay";

export class TransportWorker {
  private connection;
  private channel;
  private connected;
  constructor() {
    console.log("Starting Bus connection..");
    this.connected = false;
  }

  async connect() {
    while (!this.connected) {
      console.log("Trying to connect...");
      try {
        this.connection = await amqp.connect("amqp://guest:guest@rabbit:5672");
        this.channel = await this.connection.createChannel();
        // await this.channel.assertQueue("binance", { durable: false });
        this.connected = true;
        console.log("Rabbit Connected!!");
      } catch {
        console.log("Error on connecting Rabbit. Retrying...");
        await delay(5000);
      }
    }
    return this;
  }

  async assertQueue(line) {
    this.channel.assertQueue(line, { durable: false });
    return this.channel;
  }

  async consume(line, callback) {
    this.channel.consume(line, callback, {
      noAck: true
    });
  }
  async sendToQueue(line) {
    try {
      this.channel.sendToQueue(
        line,
        Buffer.from(JSON.stringify({ timeframe: "4h" }))
      );
    } catch (err) {
      console.log("Error writing to file...", err);
    }
  }
}
