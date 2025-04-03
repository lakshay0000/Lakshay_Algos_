from backtestTools.util import createPortfolio
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from backtestTools.histData import getEquityBacktestData, getEquityHistData
import talib
import pandas as pd
from termcolor import colored, cprint
from datetime import datetime
from backtestTools.util import setup_logger


class Horizontal50(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "Horizontal50":
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

        stocks = [
            "ADANIENT", "MRF", "BAJAJ-AUTO", "AARTIIND", "HDFCBANK", "ABB", "ABBOTINDIA", "ABCAPITAL",
            "ABFRL", "ACC", "ADANIPORTS", "AMBUJACEM", "APOLLOHOSP", "APOLLOTYRE", "ASHOKLEY",
            "ASIANPAINT", "ASTRAL", "AUROPHARMA", "AXISBANK", "BAJAJFINSV", "BAJFINANCE", "BALKRISIND",
            "BALRAMCHIN", "BANDHANBNK", "BANKBARODA", "BATAINDIA", "BEL", "BERGEPAINT", "BHARATFORG",
            "BHARTIARTL", "BHEL", "BIOCON", "BOSCHLTD", "BPCL", "BRITANNIA", "BSOFT", "CANBK",
            "CANFINHOME", "CHAMBLFERT", "CHOLAFIN", "CIPLA", "COALINDIA", "COFORGE", "COLPAL",
            "CONCOR", "COROMANDEL", "CROMPTON", "CUB", "CUMMINSIND", "DABUR", "DALBHARAT", "DEEPAKNTR",
            "DIVISLAB", "DIXON", "DLF", "DRREDDY", "EICHERMOT", "ESCORTS", "EXIDEIND", "FEDERALBNK",
            "GAIL", "GLENMARK", "GMRINFRA", "GNFC", "GODREJCP", "GODREJPROP", "GRANULES", "GRASIM",
            "GUJGASLTD", "HAVELLS", "HCLTECH", "HDFC", "HDFCAMC", "HDFCLIFE", "HEROMOTOCO", "HINDALCO",
            "HINDCOPPER", "HINDPETRO", "HINDUNILVR", "HINDZINC", "ICICIBANK", "ICICIGI", "ICICIPRULI",
            "IDFC", "IDFCFIRSTB", "IGL", "INDHOTEL", "INDIACEM", "INDIAMART", "INDIGO", "INDUSINDBK",
            "INDUSTOWER", "INFY", "IOC", "IPCALAB", "IRCTC", "ITC", "JINDALSTEL", "JKCEMENT", "JUBLFOOD",
            "KOTAKBANK", "LALPATHLAB", "LAURUSLABS", "LICHSGFIN", "LT", "LTF", "LTIM", "LTTS", "LUPIN",
            "M&M", "M&MFIN", "MANAPPURAM", "MARICO", "MARUTI", "MCX", "METROPOLIS", "MFSL", "MGL",
            "MOTHERSON", "MPHASIS", "MUTHOOTFIN", "NAUKRI", "NAVINFLUOR", "NESTLEIND", "NATIONALUM",
            "NMDC", "NTPC", "OBEROIRLTY", "OFSS", "ONGC", "PAGEIND", "PEL", "PERSISTENT", "PETRONET",
            "PFC", "PIDILITIND", "PIIND", "PNB", "POLYCAB", "POWERGRID", "PVRINOX", "RAMCOCEM", "RBLBANK",
            "RECLTD", "RELIANCE", "SAIL", "SBICARD", "SBILIFE", "SBIN", "SHREECEM", "SHRIRAMFIN",
            "SIEMENS", "SYNGENE", "SUNPHARMA", "SUNTV", "TATACHEM", "TATACOMM", "TATACONSUM", "TATAMOTORS",
            "TATAPOWER", "TATASTEEL", "TCS", "TECHM", "TORNTPHARM", "TRENT", "TVSMOTOR", "UBL",
            "ULTRACEMCO", "UPL", "VEDL", "VOLTAS", "WIPRO", "ZYDUSLIFE"
        ]

        df = {}

        for stock in stocks:

            df[stock] = getEquityBacktestData(stock, startTimeEpoch - (86400*5), endTimeEpoch, "D")

            if df[stock] is not None:
                df[stock].index = df[stock].index + 33300
                df[stock]["rsi"] = talib.RSI(df[stock]["c"], timeperiod=14)

                df[stock].dropna(inplace=True)
                df[stock] = df[stock][df[stock].index >= startTimeEpoch]
                df[stock].to_csv(f"{self.fileDir['backtestResultsCandleData']}{stock}_df.csv")

        amountPerTrade = 100000
        lastIndexTimeData = None
        Breakeven = False
        TotalTradeCanCome = 50
        ProfitAmount = 0


        for timeData in df['ADANIENT'].index:
            for stock in stocks:
                print(stock)

                stockAlgoLogic.timeData = timeData
                stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)

                # if lastIndexTimeData is not None:
                #     logger.info(f"Datetime: {stockAlgoLogic.humanTime}\tStock: {stockName}\tClose: {df[stock].at[lastIndexTimeData, 'c']}")

                stock_openPnl = stockAlgoLogic.openPnl[stockAlgoLogic.openPnl['Symbol'] == stock]

                if not stock_openPnl.empty:
                    for index, row in stock_openPnl.iterrows():
                        try:
                            stockAlgoLogic.openPnl.at[index, 'CurrentPrice'] = df[stock].at[lastIndexTimeData, "c"]
                        except Exception as e:
                            logger.error(f"Error fetching historical data for {row['Symbol']}: {e}")

                stockAlgoLogic.pnlCalculator()
                
                if not stock_openPnl.empty:
                    for index, row in stock_openPnl.iterrows():
                        if lastIndexTimeData in df[stock].index:
                            if index in stock_openPnl.index:

                                if df[stock].at[lastIndexTimeData, "rsi"] < 30 and df[stock].at[lastIndexTimeData, "c"] > row['EntryPrice']: 
                                    exitType = "TargetUsingRsi"
                                    stockAlgoLogic.exitOrder(index, exitType, df[stock].at[lastIndexTimeData, "c"])

                                elif (df[stock].at[lastIndexTimeData, "rsi"] < 30) and (row["EntryPrice"] > row["CurrentPrice"]):
                                    Breakeven = True

                                elif Breakeven and (row["EntryPrice"] <= row["CurrentPrice"]):
                                    exitType = "Breakeven"
                                    stockAlgoLogic.exitOrder(index, exitType)
                                    Breakeven = False

                if lastIndexTimeData is not None:
                    ProfitAmount= self.closedPnl['Pnl'].sum()
                    if ProfitAmount > 100000:
                        ProfitAmount = ProfitAmount - 100000
                        TotalTradeCanCome = TotalTradeCanCome + 1

                        nowTotalTrades = len(stockAlgoLogic.openPnl)
  
                if lastIndexTimeData in df[stock].index and stock_openPnl.empty:
                    nowTotalTrades = len(stockAlgoLogic.openPnl)
                    if (df[stock].at[lastIndexTimeData, "rsi"] > 60) and (stock_openPnl.empty) and (nowTotalTrades < TotalTradeCanCome):

                        entry_price = df[stock].at[lastIndexTimeData, "c"]
                        stockAlgoLogic.entryOrder(entry_price, stockName, (amountPerTrade//entry_price), "BUY")

                lastIndexTimeData = timeData
                stockAlgoLogic.pnlCalculator()
        
        if not stock_openPnl.empty:
            for index, row in stockAlgoLogic.openPnl.iterrows():
                exitType = "TimeUpExit"
                stockAlgoLogic.exitOrder(index, exitType, df[stock].at[lastIndexTimeData, "c"])
        stockAlgoLogic.pnlCalculator()

if __name__ == "__main__":
    startNow = datetime.now()

    devName = "NA"
    strategyName = "Horizontal50"
    version = "v1"

    startDate = datetime(2023, 1, 1, 9, 15)
    endDate = datetime(2023, 1, 31, 15, 30)

    portfolio = createPortfolio("/root/akashEquityBacktestAlgos/stocksList/nifty500 copy 2.md",1)

    algoLogicObj = Horizontal50(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")