import { RemoteWorker } from "./remote-worker/remote-worker";
import amqp from "amqplib";
import { createClient } from "redis";
import "dotenv/config";
import { MongoClient } from "mongodb";
import tulind from "tulind";

console.log("Starting fetcher...");

const remoteWorker = new RemoteWorker();
// Connection URL
const url = "mongodb://mongo:27017";
const mongo_client = new MongoClient(url);

// Database Name
const dbName = "binance";

let rabbit_connected = false;
let redis_connected = false;
let updated = false;
let conn;
let channel;
let collection;
let db;

const queue = "binance";

const redis_client = createClient({
  url: "redis://redis"
});

async function connectMongo() {
  // Use connect method to connect to the server
  await mongo_client.connect();
  console.log("Connected successfully to server");
  db = mongo_client.db(dbName);
  // collection = db.collection('1d');

  // the following code examples can be pasted here...

  return "done.";
}

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

const connectRabbit = async () => {
  console.log("Starting Rabbit connection...");
  while (!rabbit_connected) {
    console.log("Trying to connect...");
    try {
      conn = await amqp.connect("amqp://guest:guest@rabbit:5672");
      channel = await conn.createChannel();
      await channel.assertQueue("binance", { durable: false });
      rabbit_connected = true;
      console.log("Rabbit Connected!!");
    } catch {
      console.log("Error on connecting Rabbit. Retrying...");
      await delay(5000);
    }
  }
};

// export const getCandles = async (symbol, timeframe) => {
//   return await new Promise(function (resolve, reject) {
//     binanceClient
//       .getKlines({ symbol, interval: timeframe })
//       .then((ticks) => {
//         resolve(ticks[ticks.length - 1][0]);
//       })
//       .catch(() => {
//         console.log("REJECTED");

//         reject();
//       });
//     // binance.candlesticks(symbol, timeframe, (error, ticks, symbol) => {
//     //   // console.log("TICKS", ticks);
//     //   //   const response = ticks.map(t=> {
//     //   //     let [time, open, high, low, close, volume, closeTime, assetVolume, trades, buyBaseVolume, buyAssetVolume, ignored] = t;
//     //   //     return {time, open, high, low, close, volume, closeTime, assetVolume, trades, buyBaseVolume, buyAssetVolume, ignored}
//     //   //   })
//     //   if (ticks) {
//     //     resolve(ticks);
//     //   } else {
//     //     reject(Error("It broke"));
//     //   }
//     // });
//   });
// };

// export const getSymbolsFromExchange = async () => {
//   let ticker = await binance.prices();
//   return Object.keys(ticker);
// };

async function delay(ms) {
  // return await for better async stack trace support in case of errors.
  return await new Promise((resolve) => setTimeout(resolve, ms));
}

const getPairsFromQuote = async (assets) => {
  try {
    // const symbols = await getSymbolsFromExchange();
    const symbols = await remoteWorker.getSymbols();

    const symbolsList = [];
    assets.map((asset) => {
      const filtered = symbols.filter((pair) => pair.endsWith(asset));
      symbolsList.push(...filtered);
    });
    return symbolsList;
  } catch (err) {
    console.log("ERROR GETTING SYMBOLS");
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

const readUpdateCandlesQueue = () => {
  channel.consume(
    queue,
    async function (msg) {
      const message = JSON.parse(msg.content.toString());
      const { timeframe, new_timestamp } = message;
      console.log("Atualizing timeframe: ", timeframe);
      const pairs = await getPairsFromQuote(["BTC", "EUR"]);

      for (const pair of pairs) {
        let time_ = [];
        let close = [];
        let macd = {};
        let ma = [];
        // const resp = await getCandles(pair, timeframe || "1d");
        const resp = await remoteWorker.getKLines(pair, timeframe || "1d");
        resp.map((t) => {
          time_.push(new Date(t[0]).toLocaleString());
          close.push(t[4]);
          // let [time, open, high, low, close, volume, closeTime, assetVolume, trades, buyBaseVolume, buyAssetVolume, ignored] = t;
          return t;
        });
        if (time_.length > 2) {
          const output_cut = tulind.indicators.macd.start([12, 26, 9]);
          tulind.indicators.macd.indicator(
            [close],
            [12, 26, 9],
            function (err, results) {
              macd = {
                time: time_,
                macd: results[0],
                macd_signal: results[1],
                macd_histogram: results[2],
                output_cut
              };
            }
          );
          tulind.indicators.sma.indicator(
            [close],
            [200],
            function (err, results) {
              ma = results[0];
            }
          );
        }
        const filter = { _id: pair };
        const updateDoc = {
          $set: {
            _id: pair,
            candles: resp,
            macd,
            ma
          }
        };
        const result = await db
          .collection(timeframe)
          .updateOne(filter, updateDoc, { upsert: true });
        console.log("INSERTED: ", pair, result);
      }
    },
    {
      noAck: true
    }
  );
};

(async () => {
  await connectRabbit();
  await connectRedis();
  await connectMongo();
  readUpdateCandlesQueue();
})();
