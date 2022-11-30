import { MemoryWorker } from "./workers/memory-worker/memory-worker";
import { TransportWorker } from "./workers/transport-worker/transport-worker";
import { DBWorker } from "./workers/db-worker/db-worker";
import { RemoteWorker } from "./workers/remote-worker/remote-worker";

console.log("Starting fetcher service....");

const memoryWorker = new MemoryWorker();
const remoteWorker = new RemoteWorker();
const transportWorker = new TransportWorker();
const dbWorker = new DBWorker("klines");

const updateDB = async function (ch, msg) {
  const message = JSON.parse(msg.content.toString());
  const { timeframe, new_timestamp } = message;
  console.log("Atualizing timeframe: ", timeframe);
  const pairs = await remoteWorker.getSymbolsFromQuote(["BTC", "EUR"]);

  for (const pair of pairs) {
    const klines = await remoteWorker.getKLines(pair, timeframe || "1d");
    const result = await dbWorker.saveKLines(timeframe, pair, klines);
    transportWorker.sendToQueue("indicators", { pair, timeframe, klines });
  }
  ch.ack(msg);
};

(async () => {
  memoryWorker.connect();
  transportWorker
    .connect()
    .then((w) => w.assertQueue("klines"))
    .then((w) => w.consume("klines", (msg) => updateDB(w, msg)));
})();
