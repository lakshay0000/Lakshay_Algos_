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
            df_5min = getEquityBacktestData(
                stockName, startTimeEpoch-7776000, endTimeEpoch, "5Min",conn=conn)
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
        
        results= []
        results = taa.supertrend(df_5min["h"], df_5min["l"], df_5min["c"], length=10, multiplier=3.0)
        df_5min["Supertrend"] = results["SUPERTd_10_3.0"]
        df_5min.dropna(inplace=True)

    # Calculate the 12-period EMA
        df_5min['EMA12'] = df_5min['c'].ewm(span=12, adjust=False).mean()

    # Calculate the 26-period EMA
        df_5min['EMA26'] = df_5min['c'].ewm(span=26, adjust=False).mean()

    # Calculate MACD (the difference between 12-period EMA and 26-period EMA)
        df_5min['MACD'] = df_5min['EMA12'] - df_5min['EMA26'] 
        # Calculate the 9-period EMA of MACD (Signal Line)
        df_5min['Signal_Line'] = df_5min['MACD'].ewm(span=9, adjust=False).mean() 
        df_5min.dropna(inplace=True)

        df_5min['Mcross'] = np.where((df_5min['MACD'] > df_5min['Signal_Line']) & (df_5min['MACD'].shift(1) < df_5min['Signal_Line'].shift(1)),1,0)
        df_5min['Scross'] = 0
        df_5min.loc[(df_5min['Supertrend'] == 1) & (df_5min['Supertrend'].shift(1) == -1), 'Scross'] = 1
        df_5min.loc[(df_5min['Supertrend'] == -1) & (df_5min['Supertrend'].shift(1) == 1), 'Scross'] = -1



        # Filter dataframe from timestamp greater than start time timestamp
        df_5min = df_5min[df_5min.index > startTimeEpoch]

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{stockName}_df.csv")
        df_5min.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{stockName}_5mindf.csv")

        amountPerTrade = 100000
        lastIndexTimeData = [0, 0]
        last5MinIndexTimeData =[0, 0]


        for timeData in df.index:
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            if (timeData-300) in df_5min.index:
                last5MinIndexTimeData.pop(0)
                last5MinIndexTimeData.append(timeData-300)

            stockAlgoLogic.timeData = timeData
            stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)

            if lastIndexTimeData[1] in df.index:
                logger.info(
                    f"Datetime: {stockAlgoLogic.humanTime}\tStock: {stockName}\tClose: {df.at[lastIndexTimeData[1],'c']}")

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
                    if row["PositionStatus"] == 1:
                        if row["CurrentPrice"] <= (row["STOPLOSS"]):
                            exitType = "Stoploss Hit"
                            stockAlgoLogic.exitOrder(
                                index, exitType, (row["CurrentPrice"]))
                            
                        elif row["CurrentPrice"] >= (1.01*row["EntryPrice"]):
                            exitType = "Target Hit"
                            stockAlgoLogic.exitOrder(
                                index, exitType, (row["CurrentPrice"]))

            if ((timeData-300) in df_5min.index) & (stockAlgoLogic.openPnl.empty):
                if (df_5min.at[last5MinIndexTimeData[1], "Mcross"] == 1) &  (df_5min.at[last5MinIndexTimeData[1], "Supertrend"] == 1):
                    entry_price = df_5min.at[last5MinIndexTimeData[1], "c"]

                    stoploss = df_5min.at[last5MinIndexTimeData[1], "l"]

                    stockAlgoLogic.entryOrder(
                        entry_price, stockName,  (amountPerTrade//entry_price), "BUY", {"STOPLOSS": stoploss},)

        stockAlgoLogic.pnlCalculator()




if __name__ == "__main__":
    startNow = datetime.now()

    # Define Strategy Nomenclature
    devName = "NA"
    # Change 'strategyName' from 'rsiDmiIntraday' to 'rsiDmiOvernight' to switch between strategy
    strategyName = "rsiDmiOvernight"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2021, 1, 1, 9, 15)
    endDate = datetime(2021, 1, 25, 15, 30)
    # endDate = datetime.now()

    portfolio = createPortfolio("/root/Lakshay_Algos/stocksList/fnoWithoutNiftyStocks.md")

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