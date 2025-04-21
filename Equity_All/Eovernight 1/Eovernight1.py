import threading
import talib
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
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "rsiDmiOvernight":
            raise Exception("Strategy Name Mismatch")

        # Calculate total number of backtests
        total_backtests = sum(len(batch) for batch in portfolio)
        completed_backtests = 0
        cprint(
            f"Backtesting: {self.strategyName} UID: {self.fileDirUid}", "green")
        print(colored("Backtesting 0% complete.", "light_yellow"), end="\r")

        for batch in portfolio:
            processes = []
            for stock in batch:
                p = multiprocessing.Process(
                    target=self.backtest, args=(stock, startDate, endDate))
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

    def backtest(self, stockName, startDate, endDate):
        conn = connectToMongo()

        # Set start and end timestamps for data retrieval
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()

        stockAlgoLogic = equityOverNightAlgoLogic(stockName, self.fileDir)

        logger = setup_logger(
            stockName, f"{self.fileDir['backtestResultsStrategyLogs']}/{stockName}.log",)
        logger.propagate = False

        try:
            # Subtracting 2592000 to subtract 90 days from startTimeEpoch
            df = getEquityBacktestData(
                stockName, startTimeEpoch, endTimeEpoch, "1Min",conn=conn)
            df_1d = getEquityBacktestData(
                stockName, startTimeEpoch, endTimeEpoch, "D",conn=conn)
        except Exception as e:
        # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {stockName} in range {startDate} to {endDate}")
            raise Exception(e)   
        
        try:
            df_1d.dropna(inplace=True)
            df.dropna(inplace=True)
        except:
            self.strategyLogger.info(f"Data not found for {stockName}")
            return
        
        df_1d["ti"] = df_1d["ti"] + 33300

        df_1d.set_index("ti", inplace=True)


        # Filter dataframe from timestamp greater than start time timestamp
        df_1d = df_1d[df_1d.index > startTimeEpoch]

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{stockName}_df.csv")
        df_1d.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{stockName}_1ddf.csv")

        amountPerTrade = 100000
        lastIndexTimeData = [0, 0]
        last1DIndexTimeData =[0, 0]
        flag1=0


        for timeData in df.index:
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            if (timeData-86400) in df_1d.index:
                last1DIndexTimeData.pop(0)
                last1DIndexTimeData.append(timeData-86400)

            stockAlgoLogic.timeData = timeData
            stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)

            if last1DIndexTimeData[1] in df_1d.index and lastIndexTimeData[1] in df.index:
                logger.info(
                    f"Datetime: {stockAlgoLogic.humanTime}\tStock: {stockName}\tClose: {df_1d.at[last1DIndexTimeData[1],'c']}\tClose: {df.at[lastIndexTimeData[1],'c']}\t1dT: {last1DIndexTimeData}\t1mT: {lastIndexTimeData}\tflag1: {flag1}")

            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    try:
                        data = getEquityHistData(
                            row["Symbol"], lastIndexTimeData[1],conn=conn)
                        stockAlgoLogic.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        logging.info(e)

            stockAlgoLogic.pnlCalculator()
            
            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    if row["PositionStatus"] == 1 and (stockAlgoLogic.humanTime.time()== time(15, 00)) and (flag1==1):
                        exitType = "3:00(Timeup)"
                        stockAlgoLogic.exitOrder(
                            index, exitType, (row["CurrentPrice"]))
                        
                    flag1=0

            # if last1DIndexTimeData[1] in df_1d.index and lastIndexTimeData[1] in df.index:
            #     logger.info(f"{stockAlgoLogic.humanTime.time() == time(15, 15)}\t{df.at[lastIndexTimeData[1],"c"] >= 1.04*(df_1d.at[last1DIndexTimeData[1], "c"])}\t{(flag1==0)}")
                        

            if ((timeData-86400) in df_1d.index) & (stockAlgoLogic.openPnl.empty):

                # logger.info(f"{stockAlgoLogic.humanTime.time() == time(15, 15)}\t{df.at[lastIndexTimeData[1],"c"] >= 1.04*(df_1d.at[last1DIndexTimeData[1], "c"])}\t{(flag1==0)}")
                
                if (stockAlgoLogic.humanTime.time() == time(15, 15)) and (df.at[lastIndexTimeData[1],"c"] >= 1.005*df_1d.at[last1DIndexTimeData[1], "c"]) and (flag1==0) :
                    entry_price = df.at[lastIndexTimeData[1], "c"]

                    stockAlgoLogic.entryOrder(
                        entry_price, stockName,  (amountPerTrade//entry_price), "BUY")
                    flag1=1

        stockAlgoLogic.pnlCalculator()
        

if __name__ == "__main__":
    startNow = datetime.now()

    # Define Strategy Nomenclature
    devName = "NA"
    # Change 'strategyName' from 'rsiDmiIntraday' to 'rsiDmiOvernight' to switch between strategy
    strategyName = "rsiDmiOvernight"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2021, 1, 1, 0, 0)
    endDate = datetime(2021, 1, 25, 15, 30)
    # endDate = datetime.now()

    portfolio = createPortfolio("/root/lakshay_mudraksh/stocksList/fnoWithoutNiftyStocks.md")

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