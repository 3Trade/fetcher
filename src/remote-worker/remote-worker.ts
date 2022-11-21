import { MainClient, KlineInterval, SymbolPrice } from "binance";

const binanceClient = new MainClient({
  api_key: process.env.APIKEY,
  api_secret: process.env.APISECRET
});

export class RemoteWorker {
  async getSymbols() {
    const symbols = await binanceClient.getSymbolPriceTicker();
    if (Array.isArray(symbols)) {
      return symbols.map((symbol) => symbol.symbol);
    } else return [];
  }
  getKLines(symbol: string, timeframe: KlineInterval): Promise<any> {
    return binanceClient.getKlines({ symbol, interval: timeframe });
  }
}
