import threading
import talib
import logging
import numpy as np
import multiprocessing
from termcolor import colored, cprint
from datetime import datetime, time
from backtestTools.util import setup_logger
from backtestTools.histData import getEquityHistData
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

        # Set start and end timestamps for data retrieval
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()

        try:
            # Subtracting 2592000 to subtract 90 days from startTimeEpoch
            df = getEquityBacktestData(
                stockName, startTimeEpoch, endTimeEpoch, "1Min")
            df_5min = getEquityBacktestData(
                stockName, startTimeEpoch-7776000, endTimeEpoch, "5Min")
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
        df.dropna(inplace=True)

        for day in range(0, (endDate - startDate).days, 5):
            threads = []
            for i in range(5):
                currentDate = (
                    startDate + timedelta(days=(day+i)))

                startDatetime = datetime.combine(
                    currentDate.date(), time(9, 15, 0))
                endDatetime = datetime.combine(
                    currentDate.date(), time(15, 30, 0))

                startEpoch = startDatetime.timestamp()
                endEpoch = endDatetime.timestamp()

                currentDate5MinDf = df_5min[(df_5min.index >= startEpoch) & (df_5min.index <= endEpoch)].copy(deep=True)

                if currentDate5MinDf.empty:
                    continue
                if df.empty:
                    continue

                t = threading.Thread(
                    target=self.backtestDay, args=(stockName, startDatetime, endDatetime, currentDate5MinDf,df ))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

    def backtestDay(self, stockName, startDate, endDate,df_5min, df):
    # Set start and end timestamps for data retrieval
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()

        stockAlgoLogic = equityOverNightAlgoLogic(stockName, self.fileDir)
        stockAlgoLogic.humanTime = startDate

        logger = setup_logger(
            f"{stockName}_{stockAlgoLogic.humanTime.date()}", f"{stockAlgoLogic.fileDir['backtestResultsStrategyLogs']}{stockName}_{stockAlgoLogic.humanTime.date()}_log.log",)
        logger.propagate = False

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{stockName}_df.csv")
        df_5min.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{stockName}_5mindf.csv")


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

            # if lastIndexTimeData in df_5min.index:
            #     logger.info(
            #         f"Datetime: {stockAlgoLogic.humanTime}\tStock: {stockName}\tClose: {df_5min.at[lastIndexTimeData,'c']}")

            # Update current price for open positions
            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    try:
                        data = getEquityHistData(
                            row["Symbol"], lastIndexTimeData[1])
                        stockAlgoLogic.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        logging.info(e)

            stockAlgoLogic.pnlCalculator()
            
            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    if row["CurrentPrice"] <= (0.99*row["EntryPrice"]):
                        exitType = "Stoploss Hit"
                        stockAlgoLogic.exitOrder(
                            index, exitType, (row["CurrentPrice"]))

            if ((timeData-300) in df_5min.index) & (stockAlgoLogic.openPnl.empty):
                if (stockAlgoLogic.humanTime.time() >= time(15, 15)) & (df_5min.at[last5MinIndexTimeData[1], "rsi"] < 20):
                    entry_price = df.at[last5MinIndexTimeData[1], "c"]
                    stockAlgoLogic.entryOrder(entry_price, stockName,
                                              (amountPerTrade//entry_price), "BUY")

            # lastIndexTimeData = timeData
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
    endDate = datetime(2021, 3, 31, 15, 30)
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