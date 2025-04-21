import threading
import pandas as pd
import talib
import pandas_ta as taa
import logging
import numpy as np
import multiprocessing
from termcolor import colored, cprint
from datetime import datetime, time, timedelta
from backtestTools.util import setup_logger
from backtestTools.histData import getEquityHistData,connectToMongo
from backtestTools.histData import getEquityBacktestData
from backtestTools.algoLogic import baseAlgoLogic, equityIntradayAlgoLogic,equityOverNightAlgoLogic
from datetime import datetime, timedelta
from backtestTools.util import createPortfolio, calculateDailyReport, limitCapital, generateReportFile



rsi_upperBand = 60
rsi_lowerBand = 40
di_cross = 15




class algoLogic(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "Equity1":
            raise Exception("Strategy Name Mismatch")

        # Calculate total number of backtests
        total_backtests = sum(len(batch) for batch in portfolio)
        completed_backtests = 0
        cprint(
            f"Backtesting: {self.strategyName}_{self.version} UID: {self.fileDirUid}", "green")
        print(colored("Backtesting 0% complete.", "light_yellow"), end="\r")

        for batch in portfolio:
            processes = []
            for stock in batch:
                p = multiprocessing.Process(
                    target=self.backtestStock, args=(stock, startDate, endDate))
                p.start()
                processes.append(p)

            # Wait for all processes to finish
            for p in processes:  
                p.join()
                completed_backtests += 1
                percent_done = (completed_backtests / total_backtests) * 100
                print(colored(f"Backtesting {percent_done:.2f}% complete.", "light_yellow"), end=(
                    "\r" if percent_done != 100 else "\n"))

        return self.fileDir["backtestResultsStrategyUid"], self.combinePnlCsv()

    def backtestStock(self, stockName, startDate, endDate):
        conn = connectToMongo()
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()

        stockAlgoLogic = equityOverNightAlgoLogic(stockName, self.fileDir)
        stockAlgoLogic.humanTime = startDate

        logger = setup_logger(
            stockName, f"{self.fileDir['backtestResultsStrategyLogs']}/{stockName}.log",)
        logger.propagate = False


        try:
            # Subtracting 31540000 to subtract 1 year from startTimeEpoch
            df = getEquityBacktestData(
                stockName, startTimeEpoch, endTimeEpoch, "1min",conn=conn)
            # Subtracting 864000 to subtract 10 days from startTimeEpoch
            df_5min = getEquityBacktestData(
                stockName, startTimeEpoch-864000, endTimeEpoch, "5Min",conn=conn)
        except Exception as e:
        # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {stockName} in range {startDate} to {endDate}")
            raise Exception(e)   

        try:
            df_5min.dropna(inplace=True)
            df.dropna(inplace=True)
        except:
            self.strategyLogger.info(f"Data not found for {stockName}")
            return
        
        df_5min["rsi"] = talib.RSI(df_5min["c"], timeperiod=14)  
        df_5min.dropna(inplace=True)
        
        results= []
        results = taa.supertrend(df_5min["h"], df_5min["l"], df_5min["c"], length=10, multiplier=3.0)
        # print(results)
        df_5min["Supertrend"] = results["SUPERTd_10_3.0"]
        df_5min.dropna(inplace=True)

        df_5min['Scross'] = 0
        df_5min.loc[(df_5min['Supertrend'] == 1) & (df_5min['Supertrend'].shift(1) == -1), 'Scross'] = 1
        df_5min.loc[(df_5min['Supertrend'] == -1) & (df_5min['Supertrend'].shift(1) == 1), 'Scross'] = -1



        df_5min["rsicross60"]= np.where((df_5min["rsi"] > 60) & (df_5min["rsi"].shift(1) < 60), 1, 0)
        df_5min["rsicross40"] = np.where((df_5min["rsi"] < 40) & (df_5min["rsi"].shift(1) > 40), 1, 0)


        df_5min.dropna(inplace=True)
        df.dropna(inplace=True)

        df_5min = df_5min[df_5min.index > startTimeEpoch]

        df.to_csv(
            f"{stockAlgoLogic.fileDir['backtestResultsCandleData']}{stockName}_df.csv")
        df_5min.to_csv(
            f"{stockAlgoLogic.fileDir['backtestResultsCandleData']}{stockName}_5mindf.csv")
        

        amountPerTrade = 100000
        lastIndexTimeData = [0, 0]
        last5MinIndexTimeData = [0, 0]


        for timeData in df.index:
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            if (timeData-300) in df_5min.index:
                last5MinIndexTimeData.pop(0)
                last5MinIndexTimeData.append(timeData-300)

            stockAlgoLogic.timeData = timeData
            stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)

            # if lastIndexTimeData[1] in df.index:
            #     logger.info(
            #         f"Datetime: {stockAlgoLogic.humanTime}\tStock: {stockName}\tClose: {df.at[lastIndexTimeData[1],'c']}\tTrend: {trend}")

            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    # print(stockName)
                    stockAlgoLogic.openPnl.at[index,"CurrentPrice"] = df.at[lastIndexTimeData[1], "c"]
            
            stockAlgoLogic.pnlCalculator()        
                                                         
            # if lastIndexTimeData[1] in df.index:
            #                 logger.info(
            #                     f"Datetime: {stockAlgoLogic.humanTime}\tStock: {stockName}\tClose: {df.at[lastIndexTimeData[1],'c']}\tTrend: {trend}\tMax: {max}")


            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    
                    if stockAlgoLogic.humanTime.time() >= time(15, 15):
                        exitType = "Time Up"
                        stockAlgoLogic.exitOrder(index, exitType)

                    elif row["PositionStatus"] == 1:
                        if row["CurrentPrice"] <= (0.995*row["EntryPrice"]):
                            exitType = "Stoploss Hit"
                            stockAlgoLogic.exitOrder(
                                index, exitType, (row["CurrentPrice"]))
                    elif row["PositionStatus"] == -1:
                        if row["CurrentPrice"] >= (1.005*row["EntryPrice"]):
                            exitType = "Stoploss Hit"
                            stockAlgoLogic.exitOrder(
                                index, exitType, (row["CurrentPrice"]))
                        
                # elif row["PositionStatus"] == 1:
                #     if df.at[lastIndexTimeData[1], "l"] <= (0.995*max):
                #         exitType = "Stoploss Hit"
                #         stockAlgoLogic.exitOrder(
                #             index, exitType, df.at[lastIndexTimeData[1], "l"])
                # elif row["PositionStatus"] == -1:
                #     if df.at[lastIndexTimeData[1], "h"] >= (1.005*max):
                #         exitType = "Stoploss Hit"
                #         stockAlgoLogic.exitOrder(
                #             index, exitType, df.at[lastIndexTimeData[1], "h"])

            if (stockAlgoLogic.openPnl.empty) & (stockAlgoLogic.humanTime.time() < time(15, 15)):
                if (timeData-300) in df_5min.index:
                    if (df_5min.at[last5MinIndexTimeData[1], "rsicross60"] ==1) & (df_5min.at[last5MinIndexTimeData[1], "Scross"] == 1):
                        entry_price = df_5min.at[last5MinIndexTimeData[1], "c"]
                        stockAlgoLogic.entryOrder(
                            entry_price, stockName,  (amountPerTrade//entry_price), "BUY")

                    if (df_5min.at[last5MinIndexTimeData[1], "rsicross40"] ==1) & (df_5min.at[last5MinIndexTimeData[1], "Scross"] == -1):
                        entry_price = df_5min.at[last5MinIndexTimeData[1], "c"]
                        stockAlgoLogic.entryOrder(entry_price, stockName,
                                                  (amountPerTrade//entry_price), "SELL")


        stockAlgoLogic.pnlCalculator()      

if __name__ == "__main__":
    startNow = datetime.now()

    devName = "NA"
    strategyName = "Equity1"
    version = "v1"

    startDate = datetime(2021, 1, 1, 9, 15)
    endDate = datetime(2021, 1, 25, 15, 30) 

    portfolio = createPortfolio("/root/Lakshay_Algos/stocksList/fnoWithoutNiftyStocks.md")

    algoLogicObj = algoLogic(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    dailyReport = calculateDailyReport(closedPnl, fileDir, timeFrame=timedelta(days=1), mtm=True, fno=False)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")    