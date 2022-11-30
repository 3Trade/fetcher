import amqp from "amqplib";
import { delay } from "../../helpers/delay";

export class TransportWorker {
  private connection;
  private channel;
  private connected;
  constructor() {
    console.log("Starting Rabbit worker...");
    this.connected = false;
  }

  async connect() {
    console.log("Trying to connect Rabbit...");
    while (!this.connected) {
      try {
        this.connection = await amqp.connect("amqp://guest:guest@rabbit:5672");
        this.channel = await this.connection.createChannel();
        this.channel.prefetch(1);
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
    await this.channel.consume(line, callback, {
      noAck: false
    });
  }
  async sendToQueue(line, params) {
    try {
      this.channel.sendToQueue(line, Buffer.from(JSON.stringify(params)));
    } catch (err) {
      console.log("Error writing to file...", err);
    }
  }
}
