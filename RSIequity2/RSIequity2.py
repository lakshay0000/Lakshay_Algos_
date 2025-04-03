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
            try:
                df = getEquityBacktestData(stock, startTimeEpoch - (86400 * 150), endTimeEpoch, "D")

                if df is not None:
                    df['datetime'] = pd.to_datetime(df['datetime'])
                    df.index = df.index + 33300
                    df["rsi"] = talib.RSI(df["c"], timeperiod=14)
                    df.dropna(inplace=True)
                    df = df[df.index >= startTimeEpoch]
                    df_dict[stock] = df
                    df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{stock}_df.csv")
                    print(f"Finished processing {stock}")
                else:
                    print(f"No data found for {stock}")
            except Exception as e:
                print(f"Error processing {stock}: {e}")

        def process_stocks_in_parallel(stocks, startTimeEpoch, endTimeEpoch):
            df_dict = {}

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = {executor.submit(process_stock, stock, startTimeEpoch, endTimeEpoch, df_dict): stock for stock in stocks}

                for future in concurrent.futures.as_completed(futures):
                    future.result()

            return df_dict

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
            "GUJGASLTD", "HAVELLS", "HCLTECH", "HDFCAMC", "HDFCLIFE", "HEROMOTOCO", "HINDALCO",
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

        df_dict = process_stocks_in_parallel(stocks, startTimeEpoch, endTimeEpoch)


        amountPerTrade = 100000
        lastIndexTimeData = None
        breakeven = {}
        TotalTradeCanCome = 50
        ProfitAmount = 0

        for timeData in df_dict['ADANIENT'].index:
            for stock in stocks:
                if stock not in df_dict:
                    print(f"Skipping {stock} as no data is available.")
                    continue

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

                # if lastIndexTimeData in df_dict[stock].index:
                #     logger.info(f"Datetime: {stockAlgoLogic.humanTime}\tStock: {stockName}\tClose: {df_dict[stock].at[lastIndexTimeData, 'c']}")

                stockAlgoLogic.pnlCalculator()


                for index, row in stock_openPnl.iterrows():
                    if lastIndexTimeData in df_dict[stock].index:
                        if index in stock_openPnl.index:
                            if (df_dict[stock].at[lastIndexTimeData, "rsi"] < 30) and (df_dict[stock].at[lastIndexTimeData, "c"] > row['EntryPrice']):
                                exitType = "RsiTargetHit"
                                ProfitAmount = ProfitAmount + ((df_dict[stock].at[lastIndexTimeData, "c"] - row['EntryPrice'])*row['Quantity'])
                                stockAlgoLogic.exitOrder(index, exitType, df_dict[stock].at[lastIndexTimeData, "c"])
                                nowTotalTrades = len(stockAlgoLogic.openPnl)
                                output_string = f"{nowTotalTrades},TotalTradeCanCome:-{TotalTradeCanCome}, {df_dict[stock].at[lastIndexTimeData, 'datetime']}, RsiTargetHit: {stock} ProfitAmount: {ProfitAmount}\n"
                                with open('reposrrrt.txt', 'a') as file:
                                    file.write(output_string)
                            
                            elif (df_dict[stock].at[lastIndexTimeData, "rsi"] < 30) and (row["EntryPrice"] > df_dict[stock].at[lastIndexTimeData, "c"]):
                                breakeven[stock] = True

                            elif (breakeven.get(stock) == True) and (df_dict[stock].at[lastIndexTimeData, "c"] >= row['EntryPrice']):
                                exitType = "BreakevenExit"
                                ProfitAmount = ProfitAmount + ((df_dict[stock].at[lastIndexTimeData, "c"] - row['EntryPrice'])*row['Quantity'])
                                stockAlgoLogic.exitOrder(index, exitType, df_dict[stock].at[lastIndexTimeData, "c"])
                                nowTotalTrades = len(stockAlgoLogic.openPnl)
                                output_string = f"{nowTotalTrades},TotalTradeCanCome:-{TotalTradeCanCome}, {df_dict[stock].at[lastIndexTimeData, 'datetime']}, Breakeven: {stock}, ProfitAmount: {ProfitAmount}\n"
                                with open('reposrrrt.txt', 'a') as file:
                                    file.write(output_string)
                                breakeven[stock] = False


                if lastIndexTimeData in df_dict[stock].index:
                    nowTotalTrades = len(stockAlgoLogic.openPnl)
                    if ProfitAmount > 100000:
                        ProfitAmount = ProfitAmount - 100000
                        TotalTradeCanCome = TotalTradeCanCome + 1
                        output_string = f"{nowTotalTrades},TotalTradeCanCome:-{TotalTradeCanCome},   {df_dict[stock].at[lastIndexTimeData, 'datetime']}, ProfitIncrease: {stock}, ProfitAmount: {ProfitAmount}\n"
                        with open('reposrrrt.txt', 'a') as file:
                            file.write(output_string)

                if lastIndexTimeData in df_dict[stock].index:
                    if df_dict[stock].at[lastIndexTimeData, "rsi"] > 60 and (stock_openPnl.empty) and (nowTotalTrades < TotalTradeCanCome):
                        entry_price = df_dict[stock].at[lastIndexTimeData, "c"]
                        stockAlgoLogic.entryOrder(entry_price, stock, (amountPerTrade // entry_price), "BUY")  
                        
                        nowTotalTrades = len(stockAlgoLogic.openPnl)
                        output_string = f"{nowTotalTrades},TotalTradeCanCome:-{TotalTradeCanCome}, {df_dict[stock].at[lastIndexTimeData, 'datetime']}, Entry: {stock}\n"
                        with open('reposrrrt.txt', 'a') as file:  
                            file.write(output_string)
                            

                lastIndexTimeData = timeData
                stockAlgoLogic.pnlCalculator()

        for index, row in stockAlgoLogic.openPnl.iterrows():
            if lastIndexTimeData in df_dict[stock].index:
                if index in stockAlgoLogic.openPnl.index:
                    exitType = "TimeUp"
                    stockAlgoLogic.exitOrder(index, exitType, row['CurrentPrice'])
        stockAlgoLogic.pnlCalculator()


if __name__ == "__main__":
    startNow = datetime.now()

    devName = "NA"
    strategyName = "stockTrend"
    version = "v1"

    startDate = datetime(2019, 1, 1, 9, 15)
    endDate = datetime(2024, 1, 31, 15, 30)

    portfolio = createPortfolio("/root/Lakshay_Algos/stocksList/tes1.md", 1)

    algoLogicObj = EquityBacktest(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")