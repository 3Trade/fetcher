import { TransportWorker } from "./workers/transport-worker/transport-worker";
import { DBWorker } from "./workers/db-worker/db-worker";
import { RemoteWorker } from "./workers/remote-worker/remote-worker";
import { createClient } from "redis";
import "dotenv/config";
import { delay } from "./helpers/delay";

console.log("Starting fetcher....");

const remoteWorker = new RemoteWorker();
const dbWorker = new DBWorker("klines");
const transportWorker = new TransportWorker();

let redis_connected = false;

const redis_client = createClient({
  url: "redis://redis"
});

const connectRedis = async () => {
  console.log("Starting Redis connection...");
  while (!redis_connected) {
    console.log("Trying to connect...");
    try {
      await redis_client.connect();
      redis_connected = true;
      console.log("Redis Connected!!");
    } catch {
      console.log("Error on connecting Redis. Retrying...");
      await delay(5000);
    }
  }
};

// export const getMacd = async (symbol, timeframe) => {
//   return await new Promise(function (resolve, reject) {
//     let time = [];
//     let close = [];
//     binance.candlesticks(symbol, timeframe, (error, ticks, symbol) => {
//       ticks.map((t) => {
//         time.push(new Date(t[0]).toLocaleString());
//         close.push(t[4]);
//         // let [time, open, high, low, close, volume, closeTime, assetVolume, trades, buyBaseVolume, buyAssetVolume, ignored] = t;
//         return t;
//       });
//       if (time.length > 2) {
//         const output_cut = tulind.indicators.macd.start([12, 26, 9]);
//         tulind.indicators.macd.indicator(
//           [close],
//           [12, 26, 9],
//           function (err, results) {
//             resolve({
//               time: time_,
//               macd: results[0],
//               macd_signal: results[1],
//               macd_histogram: results[2],
//               output_cut
//             });
//           }
//         );
//       } else {
//         reject(Error("It broke"));
//       }
//     });
//   });
// };

// export const getMa = async (symbol, timeframe) => {
//   return await new Promise(function (resolve, reject) {
//     let time = [];
//     let close = [];
//     binance.candlesticks(symbol, timeframe, (error, ticks, symbol) => {
//       ticks.map((t) => {
//         time.push(new Date(t[0]).toLocaleString());
//         close.push(t[4]);
//         // let [time, open, high, low, close, volume, closeTime, assetVolume, trades, buyBaseVolume, buyAssetVolume, ignored] = t;
//         return t;
//       });
//       if (time.length > 2) {
//         const output_cut = tulind.indicators.macd.start([12, 26, 9]);
//         tulind.indicators.macd.indicator(
//           [close],
//           [12, 26, 9],
//           function (err, results) {
//             resolve({
//               time: time_,
//               macd: results[0],
//               macd_signal: results[1],
//               macd_histogram: results[2],
//               output_cut
//             });
//           }
//         );
//       } else {
//         reject(Error("It broke"));
//       }
//     });
//   });
// };

const updateDB = async function (ch, msg) {
  console.log("MSG", ch, msg);

  const message = JSON.parse(msg.content.toString());
  const { timeframe, new_timestamp } = message;
  console.log("Atualizing timeframe: ", timeframe);
  const pairs = await remoteWorker.getSymbolsFromQuote(["BTC", "EUR"]);

  for (const pair of pairs) {
    let time_ = [];
    let close = [];
    let macd = {};
    let ma = [];
    const klines = await remoteWorker.getKLines(pair, timeframe || "1d");
    const result = await dbWorker.saveKLines(timeframe, pair, klines);
    transportWorker.sendToQueue("indicators", { pair, timeframe, klines });
    // resp.map((t) => {
    //   time_.push(new Date(t[0]).toLocaleString());
    //   close.push(t[4]);
    //   // let [time, open, high, low, close, volume, closeTime, assetVolume, trades, buyBaseVolume, buyAssetVolume, ignored] = t;
    //   return t;
    // });
    // if (time_.length > 2) {
    //   const output_cut = tulind.indicators.macd.start([12, 26, 9]);
    //   tulind.indicators.macd.indicator(
    //     [close],
    //     [12, 26, 9],
    //     function (err, results) {
    //       macd = {
    //         time: time_,
    //         macd: results[0],
    //         macd_signal: results[1],
    //         macd_histogram: results[2],
    //         output_cut
    //       };
    //     }
    //   );
    //   tulind.indicators.sma.indicator([close], [200], function (err, results) {
    //     ma = results[0];
    //   });
    // }
    // const result = await dbWorker.saveKLines(timeframe, pair, klines);
    // console.log("INSERTED: ", timeframe, pair, result);
  }
  ch.ack(msg);
};

(async () => {
  await connectRedis();
  transportWorker
    .connect()
    .then((w) => w.assertQueue("klines"))
    .then((w) => w.consume("klines", (msg) => updateDB(w, msg)));
})();
