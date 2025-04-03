from backtestTools.util import createPortfolio, generateReportFile, calculateDailyReport, limitCapital
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from backtestTools.histData import getEquityBacktestData, getEquityHistData
import talib
import logging
import multiprocessing
import pandas as pd
from termcolor import colored, cprint
from datetime import datetime, timedelta, time
from backtestTools.util import setup_logger

# BLS01_H50
class Horizontal50(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "Horizontal50":
            raise Exception("Strategy Name Mismatch")
        total_backtests = sum(len(batch) for batch in portfolio)
        completed_backtests = 0
        cprint(f"Backtesting: {self.strategyName} UID: {self.fileDirUid}", "green")
        print(colored("Backtesting 0% complete.", "light_yellow"), end="\r")
        for batch in portfolio:
            processes = []
            for stock in batch:
                p = multiprocessing.Process(target=self.backtest, args=(stock, startDate, endDate))
                p.start()
                processes.append(p)
            for p in processes:
                p.join()
                completed_backtests += 1
                percent_done = (completed_backtests / total_backtests) * 100
                print(colored(f"Backtesting {percent_done:.2f}% complete.", "light_yellow"), end="\r")
        return self.fileDir["backtestResultsStrategyUid"], self.combinePnlCsv()

    def backtest(self, stockName, startDate, endDate):
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()
        stockAlgoLogic = equityOverNightAlgoLogic(stockName, self.fileDir)
        logger = setup_logger(stockName, f"{self.fileDir['backtestResultsStrategyLogs']}/{stockName}.log")
        logger.propagate = False

        stocks = [
            "AARTIIND", "ADANIENT","BAJAJ-AUTO", "HDFCBANK", "ABB", "ABBOTINDIA", "ABCAPITAL",
            "ABFRL", "ACC", "ADANIPORTS", "AMBUJACEM", "APOLLOHOSP", "APOLLOTYRE", "ASHOKLEY", "ASIANPAINT",
            "ASTRAL", "AUROPHARMA", "AXISBANK", "BAJAJFINSV", "BAJFINANCE", "BALKRISIND", "BALRAMCHIN",
            "BANDHANBNK", "BANKBARODA", "BATAINDIA", "BEL", "BERGEPAINT", "BHARATFORG", "BHARTIARTL",
            "BHEL", "BIOCON", "BOSCHLTD", "BPCL", "BRITANNIA", "BSOFT", "CANBK", "CANFINHOME", "CHAMBLFERT",
            "CHOLAFIN", "CIPLA", "COALINDIA", "COFORGE", "COLPAL", "CONCOR", "COROMANDEL", "CROMPTON", "CUB",
            "CUMMINSIND", "DABUR", "DALBHARAT", "DEEPAKNTR", "DIVISLAB", "DIXON", "DLF", "DRREDDY", "EICHERMOT",
            "ESCORTS", "EXIDEIND", "FEDERALBNK", "GAIL", "GLENMARK", "GMRINFRA", "GNFC", "GODREJCP", "GODREJPROP",
            "GRANULES", "GRASIM", "GUJGASLTD", "HAVELLS", "HCLTECH", "HDFCAMC", "HDFCLIFE", "HEROMOTOCO",
            "HINDALCO", "HINDCOPPER", "HINDPETRO", "HINDUNILVR", "HINDZINC", "ICICIBANK", "ICICIGI", "ICICIPRULI",
            "IDFC", "IDFCFIRSTB", "IGL", "INDHOTEL", "INDIACEM", "INDIGO", "INDUSINDBK",
            "IOC", "IPCALAB", "IRCTC", "ITC", "JINDALSTEL", "JKCEMENT", "JUBLFOOD", "KOTAKBANK", "LALPATHLAB",
            "LAURUSLABS", "LICHSGFIN", "LT", "LTIM", "LTTS", "LUPIN", "M&M", "M&MFIN", "MANAPPURAM", "MARICO",
            "MARUTI", "MCX", "METROPOLIS", "MFSL", "MGL", "MOTHERSON", "MPHASIS", "MUTHOOTFIN", "NAUKRI",
            "NAVINFLUOR", "NESTLEIND", "NATIONALUM", "NMDC", "NTPC", "OBEROIRLTY", "OFSS", "ONGC", "PAGEIND",
            "PEL", "PERSISTENT", "PETRONET", "PFC", "PIDILITIND", "PIIND", "PNB", "POLYCAB", "POWERGRID",
            "RBLBANK", "RECLTD", "RELIANCE", "SAIL", "SBICARD", "SBILIFE", "SBIN",
            "SHREECEM", "SHRIRAMFIN", "SIEMENS", "SYNGENE", "SUNPHARMA", "SUNTV", "TATACHEM", "TATACOMM",
            "TATACONSUM", "TATAMOTORS", "TATAPOWER", "TATASTEEL", "TCS", "TECHM", "TORNTPHARM", "TRENT",
            "TVSMOTOR", "UBL", "ULTRACEMCO", "UPL", "VEDL", "VOLTAS", "WIPRO", "ZYDUSLIFE"
        ]

        df = {}
        df_1d = {}

        for stock in stocks:
            df[stock] = getEquityBacktestData(stock, startTimeEpoch - (86400*5), endTimeEpoch, "1h")
            df[stock].index = df[stock].index + 33300
            df_1d[stock] = getEquityBacktestData(stock, startTimeEpoch - (86400*500), endTimeEpoch, "D")

            if df_1d[stock] is not None:
                df_1d[stock].index = df_1d[stock].index + 33300
                df_1d[stock]["rsi_1d"] = talib.RSI(df_1d[stock]["c"], timeperiod=14)
                df_1d[stock]['datetime'] = pd.to_datetime(df_1d[stock]['datetime'])
                df_1d[stock]['date'] = df_1d[stock]['datetime'].dt.date
                df_1d[stock] = df_1d[stock].rename(columns={'c': 'c_1d'})

                df[stock].dropna(inplace=True)

            if df[stock] is not None:
                df[stock]['datetime'] = pd.to_datetime(df[stock]['datetime'])
                df[stock]['time'] = df[stock]['datetime'].dt.strftime('%H:%M')
                df[stock]['date'] = df[stock]['datetime'].dt.date
                df[stock]['month'] = df[stock]['datetime'].dt.month
                df[stock]['year'] = df[stock]['datetime'].dt.year
                df[stock]['day'] = df[stock]['datetime'].dt.day
                df[stock]['date'] = pd.to_datetime(df[stock]['date'])
                df[stock]['yes'] = ''

                second_last_indices = df[stock].groupby(df[stock]['date'].dt.year).nth(-2).index
                df[stock].loc[second_last_indices, 'yes'] = 'yes'

                df[stock].dropna(inplace=True)

                df[stock]["rsi"] = talib.RSI(df[stock]["c"], timeperiod=14)
                df[stock]['prev_rsi'] = df[stock]['rsi'].shift(1)
                df[stock]['prev_c'] = df[stock]['c'].shift(1)

                df[stock].dropna(inplace=True)

                df[stock]['timeUp'] = ""
                df[stock].loc[df[stock].index[-1], 'timeUp'] = 'timeUp'

                df[stock].dropna(inplace=True)

                df[stock]['date'] = pd.to_datetime(df[stock]['date'])
                df_1d[stock]['date'] = pd.to_datetime(df_1d[stock]['date'])

                if df_1d[stock] is not None and 'date' in df_1d[stock]:
                    df[stock] = df[stock].merge(df_1d[stock][["date", "rsi_1d", "c_1d"]], on="date", how="left")

                if not isinstance(df[stock].index, pd.DatetimeIndex):
                    df[stock].index = pd.to_datetime(df[stock].index, unit='s')

                df[stock] = df[stock][['ti', 'datetime', 'o', 'h', 'l', 'c', 'v', 'rsi', 'prev_rsi', 'timeUp', 'rsi_1d', 'c_1d', 'date']]
                df[stock] = df[stock].reset_index(drop=True)
                
                df[stock].to_csv(f"{self.fileDir['backtestResultsCandleData']}{stock}_df.csv")

        amountPerTrade = 30000
        lastIndexTimeData = None
        breakeven = {}
        TotalTradeCanCome = 50
        ProfitAmount = 0
        entryTrigger = {}
        dateShouldBeChange = {}
        LossAmount = 0
        BufferAmount = 15000

        for timeData in df['AARTIIND'].index:
            for stock in stocks:
                print(stock)

                stockAlgoLogic.timeData = timeData
                stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)

                if lastIndexTimeData in df[stock].index:
                    logger.info(f"Datetime: {stockAlgoLogic.humanTime}\tStock: {stockName}\tClose: {df[stock].at[lastIndexTimeData, 'c']}")

                stock_openPnl = stockAlgoLogic.openPnl[stockAlgoLogic.openPnl['Symbol'] == stock]

                if not stock_openPnl.empty:
                    for index, row in stock_openPnl.iterrows():
                        try:
                            stockAlgoLogic.openPnl.at[index, 'CurrentPrice'] = df[stock].at[lastIndexTimeData, "c"]
                        except Exception as e:
                            logger.error(f"Error fetching historical data for {row['Symbol']}: {e}")

                stockAlgoLogic.pnlCalculator()

                for index, row in stock_openPnl.iterrows():
                    if lastIndexTimeData in df[stock].index: 
                        if index in stock_openPnl.index:
                            if df[stock].at[lastIndexTimeData, "timeUp"] == "timeUp":
                                exitType = "TimeUpExit"
                                stockAlgoLogic.exitOrder(index, exitType, df[stock].at[lastIndexTimeData, "c"])

                            elif breakeven.get(stock) != True and row['EntryPrice'] > df[stock].at[lastIndexTimeData, "c"] and df[stock].at[lastIndexTimeData, "rsi"] < 30:
                                breakeven[stock] = True
                                # nowTotalTrades = len(stockAlgoLogic.openPnl)

                            elif breakeven.get(stock) == True and df[stock].at[lastIndexTimeData, "c"] > row['EntryPrice']:
                                exitType = "BreakevenExit"
                                stockAlgoLogic.exitOrder(index, exitType, df[stock].at[lastIndexTimeData, "c"])
                                # breakeven[stock] = False
                                nowTotalTrades = len(stockAlgoLogic.openPnl)
                                output_string = f"{nowTotalTrades},TotalTradeCanCome:-{TotalTradeCanCome}, {df[stock].at[lastIndexTimeData, 'datetime']}, Breakeen: {stock}\n"
                                with open('reposrrrt.txt', 'a') as file:
                                    file.write(output_string)

                                if df[stock].at[lastIndexTimeData, "rsi"] > 70 and nowTotalTrades < TotalTradeCanCome:
                                    entry_price = df[stock].at[lastIndexTimeData, "c"]
                                    breakeven[stock] = False
                                    quantity = (amountPerTrade // entry_price)
                                    if ((amountPerTrade - (quantity * entry_price)) + BufferAmount) > entry_price:
                                        quantity = quantity + 1
                                    stockAlgoLogic.entryOrder(entry_price, stock, quantity, "BUY")
                                breakeven[stock] = False

                            elif df[stock].at[lastIndexTimeData, "rsi"] < 30 and df[stock].at[lastIndexTimeData, "c"] > row['EntryPrice']:
                                exitType = "TargetUsingRsi"
                                stockAlgoLogic.exitOrder(index, exitType, df[stock].at[lastIndexTimeData, "c"])
                                nowTotalTrades = len(stockAlgoLogic.openPnl)

                                PnL = (((df[stock].at[lastIndexTimeData, "c"] - row['EntryPrice']) * row['Quantity']))
                                ProfitAmount = ProfitAmount + PnL

                                output_string = f"{nowTotalTrades},TotalTradeCanCome:-{TotalTradeCanCome}, {df[stock].at[lastIndexTimeData, 'datetime']}, TargetRsi: {stock}, PnL:{PnL} ProfitAmount:- {ProfitAmount}\n"
                                with open('reposrrrt.txt', 'a') as file:
                                    file.write(output_string)

                if lastIndexTimeData is not None:
                    if ProfitAmount > 30000:
                        ProfitAmount = ProfitAmount - 30000
                        TotalTradeCanCome = TotalTradeCanCome + 1

                        nowTotalTrades = len(stockAlgoLogic.openPnl)
                        output_string = f"{nowTotalTrades},TotalTradeCanCome:-{TotalTradeCanCome}, {df[stock].at[lastIndexTimeData, 'datetime']}, ProfitIncrease: {stock}, ProfitAmount: {ProfitAmount}\n"
                        with open('reposrrrt.txt', 'a') as file:
                            file.write(output_string)

                if lastIndexTimeData in df[stock].index:

                    nowTotalTrades = len(stockAlgoLogic.openPnl)
                    if df[stock].at[lastIndexTimeData, "rsi"] > 60 and stock_openPnl.empty and nowTotalTrades < TotalTradeCanCome:

                        entry_price = df[stock].at[lastIndexTimeData, "c"]
                        breakeven[stock] = False
                        quantity = (amountPerTrade // entry_price)
                        if ((amountPerTrade - (quantity * entry_price)) + BufferAmount) > entry_price:
                            quantity = quantity + 1
                        stockAlgoLogic.entryOrder(entry_price, stock, quantity, "BUY", {"quantity": (amountPerTrade // entry_price)})

                        nowTotalTrades = len(stockAlgoLogic.openPnl)
                        output_string = f"{nowTotalTrades},TotalTradeCanCome:-{TotalTradeCanCome}, {df[stock].at[lastIndexTimeData, 'datetime']}, Entry: {stock}\n"
                        with open('reposrrrt.txt', 'a') as file:
                            file.write(output_string)

                lastIndexTimeData = timeData
                stockAlgoLogic.pnlCalculator()


if __name__ == "__main__":
    startNow = datetime.now()

    devName = "NA"
    strategyName = "Horizontal50"
    version = "v1"

    startDate = datetime(2018, 1, 1, 9, 15)
    endDate = datetime(2024, 12, 31, 15, 30)

    portfolio = createPortfolio("/root/akashEquityBacktestAlgos/stocksList/test1.md",1)

    algoLogicObj = Horizontal50(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")