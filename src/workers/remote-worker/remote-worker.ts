import { MainClient, KlineInterval } from "binance";

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

  async getSymbolsFromQuote(assets: string[]): Promise<string[]> {
    try {
      const symbols = await this.getSymbols();
      const symbolsList = [];
      assets.map((asset) => {
        const filtered = symbols.filter((pair) => pair.endsWith(asset));
        symbolsList.push(...filtered);
      });
      return symbolsList;
    } catch (err) {
      console.log("ERROR GETTING SYMBOLS");
    }
  }
  getKLines(symbol: string, timeframe: KlineInterval): Promise<any> {
    return binanceClient.getKlines({ symbol, interval: timeframe });
  }
}
