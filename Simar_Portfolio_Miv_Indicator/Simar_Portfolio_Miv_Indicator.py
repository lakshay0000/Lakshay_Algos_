import threading
import talib as ta
import pandas_ta as taa
import logging
import numpy as np
import multiprocessing
from termcolor import colored, cprint
from datetime import datetime, time
from backtestTools.util import setup_logger
from backtestTools.histData import getEquityHistData,connectToMongo
from backtestTools.histData import getEquityBacktestData
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from datetime import datetime, timedelta
from backtestTools.util import createPortfolio, calculateDailyReport, limitCapital, generateReportFile


class algoLogic(baseAlgoLogic):
    def runBacktest(stockAlgoLogic, portfolio, startDate, endDate):
        if stockAlgoLogic.strategyName != "EquityOvernight":
            raise Exception("Strategy Name Mismatch")

        # Calculate total number of backtests
        total_backtests = sum(len(batch) for batch in portfolio)
        completed_backtests = 0
        cprint(
            f"Backtesting: {stockAlgoLogic.strategyName} UID: {stockAlgoLogic.fileDirUid}", "green")
        print(colored("Backtesting 0% complete.", "light_yellow"), end="\r")

        for batch in portfolio:
            processes = []
            for stock in batch:
                p = multiprocessing.Process(
                    target=stockAlgoLogic.backtest, args=(stock, startDate, endDate))
                p.start()
                processes.append(p)

            # Wait for all processes to finish
            for p in processes:
                p.join()
                completed_backtests += 1
                percent_done = (completed_backtests / total_backtests) * 100
                print(colored(f"Backtesting {percent_done:.2f}% complete.", "light_yellow"), end=(
                    "\r" if percent_done != 100 else "\n"))

        return stockAlgoLogic.fileDir["backtestResultsStrategyUid"], stockAlgoLogic.combinePnlCsv()

    def backtest(stockAlgoLogic, stockName, startDate, endDate):
        conn = connectToMongo()

        # Set start and end timestamps for data retrieval
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()

        stockAlgoLogic = equityOverNightAlgoLogic(stockName, stockAlgoLogic.fileDir)

        # logger = setup_logger("strategyLogger",f"{stockAlgoLogic.fileDir['backtestResultsStrategyLogs']}{stockName}_log.log",)
        # logger.propagate = False

        try:
            # Subtracting 2592000 to subtract 90 days from startTimeEpoch
            df = getEquityBacktestData(
                stockName, startTimeEpoch-(86400*50), endTimeEpoch, "1Min",conn=conn)
            df_1d = getEquityBacktestData(stockName, startTimeEpoch-(86400*50), endTimeEpoch, "1D",conn=conn)
        except Exception as e:
        # Log an exception if data retrieval fails
            stockAlgoLogic.strategyLogger.info(
                f"Data not found for {stockName} in range {startDate} to {endDate}")
            raise Exception(e)   
        
        try:
            df.dropna(inplace=True)
            df_1d.dropna(inplace=True)
        except:
            stockAlgoLogic.strategyLogger.info(f"Data not found for {stockName}")
            return
        
        # Calculate the 20-period EMA
        df['EMA10'] = df['c'].ewm(span=10, adjust=False).mean()        
        df_1d['EMA10'] = df_1d['c'].ewm(span=10, adjust=False).mean()   

        # Calculate RSI indicator
        df["rsi"] = ta.RSI(df["c"], timeperiod=14)
        df.dropna(inplace=True)

        df['ADX'] = ta.ADX(df['h'], df['l'], df['c'], timeperiod=14)

        df["ADXDown"] = np.where((df["ADX"] < df["ADX"].shift(1)), 1, 0)
        df["ADXUp"] = np.where((df["ADX"] > df["ADX"].shift(1)), 1, 0)

        # Determine crossover signals
        df["EMADown"] = np.where((df["EMA10"] < df["EMA10"].shift(1)), 1, 0)
        df["EMAUp"] = np.where((df["EMA10"] > df["EMA10"].shift(1)), 1, 0)

        df = df[df.index >= startTimeEpoch]

        # # Determine crossover signals
        # df_1d["EMADown"] = np.where((df_1d["EMA10"].shift(1) < df_1d["EMA10"].shift(2)), 1, 0)
        # df_1d["EMAUp"] = np.where((df_1d["EMA10"].shift(1) > df_1d["EMA10"].shift(2)), 1, 0)

        # Add 33360 to the index to match the timestamp
        df_1d.index = df_1d.index + 33360
        df_1d.ti = df_1d.ti + 33360

        df_1d = df_1d[df_1d.index >= ((startTimeEpoch-86340)-(86400*5))]


        df.to_csv(
            f"{stockAlgoLogic.fileDir['backtestResultsCandleData']}{stockName}_1Min.csv")
        df_1d.to_csv(
            f"{stockAlgoLogic.fileDir['backtestResultsCandleData']}{stockName}_1D.csv")

        # Strategy Parameters
        lastIndexTimeData = [0, 0]
        amountPerTrade = 100000
        TradeLimit = 0
        high_list = []
        low_list = []
        High= None
        Low = None
        Range = None
        Bullish_Day = False
        Bearish_Day = False



        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            stockAlgoLogic.timeData = timeData
            stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)
            print(stockAlgoLogic.humanTime)


            # Skip time periods outside trading hours
            if (stockAlgoLogic.humanTime.time() < time(9, 16)) | (stockAlgoLogic.humanTime.time() > time(15, 30)):
                continue

            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)

            # Strategy Specific Trading Time
            if (stockAlgoLogic.humanTime.time() < time(9, 16)) | (stockAlgoLogic.humanTime.time() > time(15, 25)):
                continue

            if lastIndexTimeData[1] not in df.index:
                continue

            #  # Log relevant information
            # if lastIndexTimeData[1] in df.index:
            #     stockAlgoLogic.strategyLogger.info(f"Datetime: {stockAlgoLogic.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")

            # if (stockAlgoLogic.humanTime.time() == time(9, 16)):
            #     TradeLimit = 0
            #     high_list = []
            #     low_list = []
            #     High= None
            #     Low = None
            #     Range = None
            #     Bullish_Day = False
            #     Bearish_Day = False

            #     if df_1d.at[timeData, 'EMADown'] == 1:
            #         Bearish_Day = True
            #         Bullish_Day = False
            #         stockAlgoLogic.strategyLogger.info(f"{stockAlgoLogic.humanTime} {stockName} Bearish Day detected.")

            #     elif df_1d.at[timeData, 'EMAUp'] == 1:
            #         Bullish_Day = True
            #         Bearish_Day = False
            #         stockAlgoLogic.strategyLogger.info(f"{stockAlgoLogic.humanTime} {stockName} Bullish Day detected.")


            
            if (stockAlgoLogic.humanTime.time() > time(9, 16)) and (stockAlgoLogic.humanTime.time() <= time(9, 21)):
                high_list.append(df.at[lastIndexTimeData[1], "h"])
                low_list.append(df.at[lastIndexTimeData[1], "l"])
                if (stockAlgoLogic.humanTime.time() == time(9, 21)):
                    High = max(high_list)
                    Low = min(low_list)
                    Range = High-Low
                    if Range < 0.002 * (df.at[lastIndexTimeData[1], "o"]):
                        Range = 0.002 * (df.at[lastIndexTimeData[1], "o"])
                        stockAlgoLogic.strategyLogger.info(f"{stockAlgoLogic.humanTime} {stockName} ATR Range too low, setting to 0.2% of open price: {Range}")
                    stockAlgoLogic.strategyLogger.info(f"{stockAlgoLogic.humanTime} {stockName} Range: {Range} High: {High} Low: {Low}")



            # Update current price for open positions
            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    if lastIndexTimeData[1] in df.index:
                        stockAlgoLogic.openPnl.at[index,"CurrentPrice"] = df.at[lastIndexTimeData[1], "c"]

            # Calculate and update PnL
            stockAlgoLogic.pnlCalculator()



            
            # Check for exit conditions and execute exit orders
            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():

                    # symSide = row["Symbol"]
                    # symSide = symSide[len(symSide) - 2:] 

                    if stockAlgoLogic.humanTime.time() >= time(15, 20):
                        exitType = "Time Up"
                        stockAlgoLogic.exitOrder(index, exitType)

                    elif (row["PositionStatus"]==1):
                            
                        if df.at[lastIndexTimeData[1], "rsi"] < 40 and df.at[lastIndexTimeData[1], 'EMADown'] == 1: 
                            if row["CurrentPrice"] > row["EntryPrice"]:
                                exitType = "Buy_Exit_Signal"
                                stockAlgoLogic.exitOrder(index, exitType)
                            elif df.at[lastIndexTimeData[1], "rsi"] < 30:
                                exitType = "Buy_Exit_Signal"
                                stockAlgoLogic.exitOrder(index, exitType)



                    elif (row["PositionStatus"]==-1):

                        if df.at[lastIndexTimeData[1], "rsi"] > 60 and df.at[lastIndexTimeData[1], 'EMAUp'] == 1:  
                            if row["CurrentPrice"] < row["EntryPrice"]:
                                exitType = "Sell_Exit_Signal"
                                stockAlgoLogic.exitOrder(index, exitType)
                            elif df.at[lastIndexTimeData[1], "rsi"] > 70:
                                exitType = "Sell_Exit_Signal"
                                stockAlgoLogic.exitOrder(index, exitType)


            # tradecount = stockAlgoLogic.openPnl['Symbol'].value_counts()
            # state["stockcount"]= tradecount.get(stockName, 0)

            # if (stockAlgoLogic.humanTime.time() < time(9, 21)):
            #     continue  

            # if Bullish_Day:
            #     if df.at[lastIndexTimeData[1], 'EMA10'] < Low:
            #         Low = df.at[lastIndexTimeData[1], 'EMA10']
            #         High = Low + Range
            #         stockAlgoLogic.strategyLogger.info(f"{stockAlgoLogic.humanTime} {stockName} New Low: {Low}, High: {High}")

            # if Bearish_Day:
            #     if df.at[lastIndexTimeData[1], 'EMA10'] > High:
            #         High = df.at[lastIndexTimeData[1], 'EMA10']
            #         Low = High - Range
            #         stockAlgoLogic.strategyLogger.info(f"{stockAlgoLogic.humanTime} {stockName} New High: {High}, Low: {Low}")


            
            if ((timeData-60) in df.index) and stockAlgoLogic.openPnl.empty and (stockAlgoLogic.humanTime.time() < time(15, 20)):

                if df.at[lastIndexTimeData[1], "rsi"] > 60 and df.at[lastIndexTimeData[1], 'ADXUp'] == 1 and df.at[lastIndexTimeData[1], 'EMAUp'] == 1:

                    entry_price = df.at[lastIndexTimeData[1], "c"]

                    stockAlgoLogic.entryOrder(entry_price, stockName, (amountPerTrade//entry_price), "BUY")
                            
                    
                        
                if df.at[lastIndexTimeData[1], "rsi"] < 40 and df.at[lastIndexTimeData[1], 'ADXUp'] == 1 and df.at[lastIndexTimeData[1], 'EMADown'] == 1:

                    entry_price = df.at[lastIndexTimeData[1], "c"]
                    
                    stockAlgoLogic.entryOrder(entry_price, stockName, (amountPerTrade//entry_price), "SELL")  
                            

            # if ((timeData-300) in df_15min.index) & (stockAlgoLogic.openPnl.empty) & (stockAlgoLogic.humanTime.time() > time(9, 30)):
            #     if (df_15min.at[last5MinIndexTimeData[1], "c"] > breakp) & (df_15min.at[last5MinIndexTimeData[1], "Scross"] == 1):
            #         entry_price = df_15min.at[last15MinIndexTimeData[1], "c"]

            #         stockAlgoLogic.entryOrder(
            #             entry_price, stockName,  (amountPerTrade//entry_price), "BUY")

        stockAlgoLogic.pnlCalculator()




if __name__ == "__main__":
    startNow = datetime.now()

    # Define Strategy Nomenclature
    devName = "NA"
    # Change 'strategyName' from 'rsiDmiIntraday' to 'rsiDmiOvernight' to switch between strategy
    strategyName = "EquityOvernight"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2025, 1, 1, 9, 15)
    endDate = datetime(2025, 8, 30, 15, 30)
    # endDate = datetime.now()

    portfolio = createPortfolio("/root/Lakshay_Algos/stocksList/nifty50.md",10)

    algoLogicObj = algoLogic(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(
        portfolio, startDate, endDate)


    dailyReport = calculateDailyReport(
        closedPnl, fileDir, timeFrame=timedelta(days=1), mtm=True, fno=False)
    # dailyReport = calculateDailyReport(
    #     closedPnl, fileDir, timeFrame=timedelta(days=1), mtm=True)

    # limitCapital(closedPnl, fileDir, maxCapitalAmount=100000)

    # generateReportFile(dailyReport, fileDir)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")