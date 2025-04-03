from backtestTools.util import createPortfolio
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from backtestTools.histData import getEquityBacktestData
import talib
import concurrent.futures
import threading
import pandas as pd
from termcolor import colored, cprint
from datetime import datetime
from backtestTools.util import setup_logger

class EquityBacktest(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "stockTrend":
            raise Exception("Strategy Name Mismatch")
        
        cprint(f"Backtesting: {self.strategyName} UID: {self.fileDirUid}", "green")
        first_stock = portfolio[0][0] if portfolio and portfolio[0] else None
        
        if first_stock:
            self.backtest(first_stock, startDate, endDate)
            print(colored("Backtesting 100% complete.", "light_yellow"))
        else:
            print(colored("No stocks to backtest.", "red"))
        
        return self.fileDir["backtestResultsStrategyUid"], self.combinePnlCsv()

    def backtest(self, stockName, startDate, endDate):
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()
        stockAlgoLogic = equityOverNightAlgoLogic(stockName, self.fileDir)
        logger = setup_logger(stockName, f"{self.fileDir['backtestResultsStrategyLogs']}/{stockName}.log")
        logger.propagate = False

        def process_stock(stock, startTimeEpoch, endTimeEpoch, df_dict):
            df = getEquityBacktestData(stock, startTimeEpoch - (86400 * 50), endTimeEpoch, "D")

            if df is not None:
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.index = df.index + 33300
                df["rsi"] = talib.RSI(df["c"], timeperiod=14)
                df.dropna(inplace=True)
                df = df[df.index > startTimeEpoch]
                df_dict[stock] = df
                df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{stock}_df.csv")
                print(f"Finished processing {stock}")

        def process_stocks_in_parallel(stocks, startTimeEpoch, endTimeEpoch):
            df_dict = {}

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = {executor.submit(process_stock, stock, startTimeEpoch, endTimeEpoch, df_dict): stock for stock in stocks}

                for future in concurrent.futures.as_completed(futures):
                    future.result()

            return df_dict

        stocks = ["ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BEL", "BHARTIARTL", 
                  "BPCL", "BRITANNIA", "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", 
                  "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "INDUSINDBK", "INFY", "ITC", "JSWSTEEL", "KOTAKBANK", "LT", 
                  "M&M", "MARUTI", "NESTLEIND", "NTPC", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SHRIRAMFIN", 
                  "SUNPHARMA", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TCS", "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO"]

        df_dict = process_stocks_in_parallel(stocks, startTimeEpoch, endTimeEpoch)


        amountPerTrade = 100000
        lastIndexTimeData = None

        for timeData in df_dict['ADANIENT'].index:
            for stock in stocks:
                stockAlgoLogic.timeData = timeData
                stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)
                print(stock, stockAlgoLogic.humanTime)

                stock_openPnl = stockAlgoLogic.openPnl[stockAlgoLogic.openPnl['Symbol'] == stock]

                if not stock_openPnl.empty:
                    for index, row in stock_openPnl.iterrows():
                        try:
                            stockAlgoLogic.openPnl.at[index, 'CurrentPrice'] = df_dict[stock].at[lastIndexTimeData, "c"]
                        except Exception as e:
                            print(f"Error fetching historical data for {row['Symbol']}")

                stockAlgoLogic.pnlCalculator()

                for index, row in stock_openPnl.iterrows():
                    if lastIndexTimeData in df_dict[stock].index:
                        if index in stock_openPnl.index:
                            if df_dict[stock].at[lastIndexTimeData, "rsi"] < 30 and df_dict[stock].at[lastIndexTimeData, "c"] > row['EntryPrice']:
                                exitType = "RsiTargetHit"
                                stockAlgoLogic.exitOrder(index, exitType, df_dict[stock].at[lastIndexTimeData, "c"])

                if lastIndexTimeData in df_dict[stock].index:
                    if df_dict[stock].at[lastIndexTimeData, "rsi"] > 60 and (stock_openPnl.empty):
                        entry_price = df_dict[stock].at[lastIndexTimeData, "c"]
                        stockAlgoLogic.entryOrder(entry_price, stock, (amountPerTrade // entry_price), "BUY")

                lastIndexTimeData = timeData
                stockAlgoLogic.pnlCalculator()

        for index, row in stockAlgoLogic.openPnl.iterrows():
            if lastIndexTimeData in df_dict[stock].index:
                if index in stockAlgoLogic.openPnl.index:
                    exitType = "TimeUp"
                    stockAlgoLogic.exitOrder(index, exitType, row['CurrentPrice'])


if __name__ == "__main__":
    startNow = datetime.now()

    devName = "AK"
    strategyName = "stockTrend"
    version = "v1"

    startDate = datetime(2019, 1, 1, 9, 15)
    endDate = datetime(2024, 12, 31, 15, 30)

    portfolio = createPortfolio("/root/equityResearch/stocksList/test1.md", 1)

    algoLogicObj = EquityBacktest(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")