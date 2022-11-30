import { createClient } from "redis";
import { delay } from "../../helpers/delay";

const redis_client = createClient({
  url: "redis://redis"
});

export class MemoryWorker {
  private connected;
  constructor() {
    console.log("Starting Redis worker...");
    this.connected = false;
  }
  async connect() {
    console.log("Trying to connect Redis...");
    while (!this.connected) {
      try {
        await redis_client.connect();
        this.connected = true;
        console.log("Redis Connected!!");
      } catch {
        console.log("Error on connecting Redis. Retrying...");
        await delay(5000);
      }
    }
  }
}
