import threading
import pandas as pd
import talib
import logging
import numpy as np
import multiprocessing
from termcolor import colored, cprint
from datetime import datetime, time, timedelta
from backtestTools.util import setup_logger
from backtestTools.histData import getEquityHistData
from backtestTools.histData import getEquityBacktestData
from backtestTools.algoLogic import baseAlgoLogic, equityIntradayAlgoLogic
from datetime import datetime, timedelta
from backtestTools.util import createPortfolio, calculateDailyReport, limitCapital, generateReportFile





class algoLogic(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "BTST":
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
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()

        try:
            # Subtracting 31540000 to subtract 1 year from startTimeEpoch
            df_1d = getEquityBacktestData(
                stockName, startTimeEpoch-3154000, endTimeEpoch, "D")
            # Subtracting 864000 to subtract 10 days from startTimeEpoch
            df_1m = getEquityBacktestData(
                stockName, startTimeEpoch-86400, endTimeEpoch, "1Min")
        except Exception as e:
            raise Exception(e)

        try:
            df_1m.dropna(inplace=True)
            df_1d.dropna(inplace=True)
        except:
            self.strategyLogger.info(f"Data not found for {stockName}")
            return

        df_1d["ti"] = df_1d["ti"] + 33300

        df_1m.set_index("ti", inplace=True)
        df_1d.set_index("ti", inplace=True)

        df_1d["ema10"] = talib.EMA(df_1d["c"], timeperiod=10)
        df_1d["ema110"] = talib.EMA(df_1d["c"], timeperiod=50)

        df_1m.dropna(inplace=True)
        df_1d.dropna(inplace=True)

        for day in range(0, (endDate - startDate).days, 5):
            threads = []
            for i in range(5):
                currentDate = (
                    startDate + timedelta(days=(day+i)))

                startDatetime = datetime.combine(
                    currentDate.date(), time(9, 15))
                endDatetime = datetime.combine(
                    currentDate.date(), time(15, 30))

                startEpoch = startDatetime.timestamp()
                endEpoch = endDatetime.timestamp()

                currentDate1MinDf = df_1m[(df_1m.index >= startEpoch) & (
                    df_1m.index <= endEpoch)].copy(deep=True)
                if currentDate1MinDf.empty:
                    continue

                df1DBeforeCurrentDate = df_1d[df_1d.index <= endEpoch]
                try:
                    trend = 1 if df1DBeforeCurrentDate.at[df1DBeforeCurrentDate.index[-2],
                                                          "ema10"] > df1DBeforeCurrentDate.at[df1DBeforeCurrentDate.index[-2], "ema110"] else -1
                except Exception as e:
                    trend = 0

                t = threading.Thread(
                    target=self.backtestDay, args=(stockName, startDatetime, endDatetime, currentDate1MinDf,df1DBeforeCurrentDate, trend))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

    def backtestDay(self, stockName, startDate, endDate, df,df_1d, trend):
        # Set start and end timestamps for data retrieval
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()

        stockAlgoLogic = equityIntradayAlgoLogic(stockName, self.fileDir)
        stockAlgoLogic.humanTime = startDate
  
        logger = setup_logger(
            f"{stockName}_{stockAlgoLogic.humanTime.date()}", f"{stockAlgoLogic.fileDir['backtestResultsStrategyLogs']}{stockName}_{stockAlgoLogic.humanTime.date()}_log.log",)
        logger.propagate = False

        df.to_csv(
            f"{stockAlgoLogic.fileDir['backtestResultsCandleData']}{stockName}_{stockAlgoLogic.humanTime.date()}_df.csv")
        df_1d.to_csv(
            f"{stockAlgoLogic.fileDir['backtestResultsCandleData']}{stockName}_{stockAlgoLogic.humanTime.date()}_df_1d.csv")

        amountPerTrade = 100000
        lastIndexTimeData = [0, 0]
        last1dIndexTimeData = [0, 0]
        flag1=0
        


        for timeData in df.index:
            stockAlgoLogic.timeData = timeData
            stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)


            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    stockAlgoLogic.openPnl.at[index,
                                              "CurrentPrice"] = df.at[lastIndexTimeData[1], "c"]
            
            stockAlgoLogic.pnlCalculator()          
                    

            
            if lastIndexTimeData[1] in df.index and last1dIndexTimeData[1] in df_1d.index:
                            
                            logger.info(
                                f"Datetime: {stockAlgoLogic.humanTime}\tStock: {stockName}\tClose: {df.at[lastIndexTimeData[1],'c']}\tTrend: {trend}\tClose1D: {df_1d.at[last1dIndexTimeData[1],'c']}\tepoch1d: {last1dIndexTimeData[1]}\tepoch: {lastIndexTimeData[1]}")
                            

            
            # Check for exit conditions and execute exit orders
            for index, row in stockAlgoLogic.openPnl.iterrows():
                
                if stockAlgoLogic.humanTime.time() >= time(15, 15) and flag1==1 and (row["EntryTime"] < df.at[lastIndexTimeData[1],"datetime"]):
                    exitType = "Time Up"
                    stockAlgoLogic.exitOrder(index, exitType)
                    flag1=0

            
            # Check for entry signals and execute orders     
            if (stockAlgoLogic.openPnl.empty & stockAlgoLogic.closedPnl.empty) & (last1dIndexTimeData[1] in df_1d.index) & (lastIndexTimeData[1] in df.index):
                if trend == 1:
                    if (stockAlgoLogic.humanTime.time() >= time(15, 15)) and (df.at[lastIndexTimeData[1],"c"] >= 1.04*(df_1d.at[last1dIndexTimeData[1], "c"])) and flag1==0 :
                        entry_price = df.at[lastIndexTimeData[1], "c"]
                        stockAlgoLogic.entryOrder(
                            entry_price, stockName,  (amountPerTrade//entry_price), "BUY")
                        flag1=1

            # Update lastIndexTimeData  
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData)
            if (timeData) in df_1d.index:
                last1dIndexTimeData.pop(0)
                last1dIndexTimeData.append(timeData)

            stockAlgoLogic.pnlCalculator()      

if __name__ == "__main__":
    startNow = datetime.now()

    devName = "NA"
    strategyName = "BTST"
    version = "v1"

    startDate = datetime(2021, 1, 1, 9, 15)
    endDate = datetime(2021, 3, 31, 15, 30)

    portfolio = createPortfolio("/root/lakshay_mudraksh/stocksList/fnoWithoutNiftyStocks.md")

    algoLogicObj = algoLogic(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    dailyReport = calculateDailyReport(closedPnl, fileDir, timeFrame=timedelta(days=1), mtm=True, fno=False)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")    