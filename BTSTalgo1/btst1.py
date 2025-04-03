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



class rsiDmiOvernightStrategy(baseAlgoLogic):
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

        stockAlgoLogic = equityOverNightAlgoLogic(stockName, self.fileDir)

        logger = setup_logger(
            stockName, f"{self.fileDir['backtestResultsStrategyLogs']}/{stockName}.log",)
        logger.propagate = False

        try:
            # Subtracting 2592000 to subtract 90 days from startTimeEpoch
            df = getEquityBacktestData(
                stockName, startTimeEpoch, endTimeEpoch, "D")
            df_1min = getEquityBacktestData(
                stockName, startTimeEpoch, endTimeEpoch, "1MIN")
        except Exception as e:
            raise Exception(e)
        
        try:
            df.dropna(inplace=True)
            df_1min.dropna(inplace=True)
        except:
            self.strategyLogger.info(f"Data not found for {stockName}")
            return

        df.index = df.index + 33300

        # Filter dataframe from timestamp greater than start time timestamp
        df = df[df.index > startTimeEpoch]
        df_1min = df_1min[df_1min.index > startTimeEpoch]

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{stockName}_df.csv")
        df_1min.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{stockName}_1mindf.csv")

        amountPerTrade = 100000
        lastIndexTimeData = [0,0]
        last1dIndexTimeData = None
        flag1=0
        # timef=0

        for timeData in df_1min.index:
            stockAlgoLogic.timeData = timeData
            stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)

            if lastIndexTimeData[1] in df_1min.index:
                logger.info(
                    f"Datetime: {stockAlgoLogic.humanTime}\tStock: {stockName}\tClose: {df_1min.at[lastIndexTimeData,'c']}")

            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    try:
                        data = getEquityHistData(
                            row['Symbol'], timeData)
                        stockAlgoLogic.openPnl.at[index,
                                                  'CurrentPrice'] = data['c']
                    except Exception as e:
                        logging.info(e)

            stockAlgoLogic.pnlCalculator()

            # if stockAlgoLogic.humanTime.time() ==time(9,15) and timef==0:
            #     timef=1
            # elif stockAlgoLogic.humanTime.time() >= time(15, 30)  and timef==1 :
            #     timef=0


            for index, row in stockAlgoLogic.openPnl.iterrows():
                if lastIndexTimeData[1] in df_1min.index:
                    if stockAlgoLogic.humanTime.time() >= time(15, 15) and flag1==1:
                        exitType = "Time Up"
                        stockAlgoLogic.exitOrder(index, exitType)
                        flag1=0


            if (stockAlgoLogic.openPnl.empty):
                
                # if (stockAlgoLogic.openPnl.empty) &  (last1dIndexTimeData[1] in df.index) & (lastIndexTimeData[1] in df_1min.index):
                if (stockAlgoLogic.openPnl.empty) &  (last1dIndexTimeData is not None) & (lastIndexTimeData is not None):
                    if (stockAlgoLogic.humanTime.time() == time(15, 15)) and (df_1min.at[lastIndexTimeData[1],"c"] >= 1.04*(df.at[last1dIndexTimeData[1], "c"])) and flag1==0 :
                        entry_price = df.at[lastIndexTimeData[1], "c"]
                        stockAlgoLogic.entryOrder(
                            entry_price, stockName,  (amountPerTrade//entry_price), "BUY")
                        flag1=1
            stockAlgoLogic.pnlCalculator()
 

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

    algoLogicObj = rsiDmiOvernightStrategy(devName, strategyName, version)
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