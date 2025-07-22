from backtestTools.util import createPortfolio, calculateDailyReport, limitCapital, generateReportFile
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from backtestTools.histData import getEquityBacktestData
from backtestTools.util import setup_logger, calculate_mtm
from datetime import datetime, timedelta
from termcolor import colored, cprint
from datetime import datetime, time
import multiprocessing
import numpy as np
import logging
import talib
import pandas_ta as ta
import pandas as pd


class equityDelta(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "equityDelta":
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
                print(colored(f"Backtesting {percent_done:.2f}% complete.", "light_yellow"), end=("\r" if percent_done != 100 else "\n"))
        return self.fileDir["backtestResultsStrategyUid"], self.combinePnlCsv()

    def backtest(self, stockName, startDate, endDate):

        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()

        stockAlgoLogic = equityOverNightAlgoLogic(stockName, self.fileDir)

        logger = setup_logger(stockName, f"{self.fileDir['backtestResultsStrategyLogs']}/{stockName}.log",)
        logger.propagate = False

        try:
            df = getEquityBacktestData("INDIA VIX", startTimeEpoch-(86400*500), endTimeEpoch, "5Min")
        except Exception as e:
            print(stockName)
            raise Exception(e)
        print(df)

        df.dropna(inplace=True)

        supertrend_one = ta.supertrend(df["h"], df["l"], df["c"], length=100, multiplier=3.6)
        supertrend_two = ta.supertrend(df["h"], df["l"], df["c"], length=100, multiplier=2.7)
        supertrend_three = ta.supertrend(df["h"], df["l"], df["c"], length=100, multiplier=1.8)

        df['SupertrendColourOne'] = supertrend_one['SUPERTd_100_3.6']
        df['SupertrendColourTwo'] = supertrend_two['SUPERTd_100_2.7']
        df['SupertrendColourThree'] = supertrend_three['SUPERTd_100_1.8']

        df['RSI'] = talib.RSI(df['c'], timeperiod=14)
        df['StochRSI_K'] = (
            (df['RSI'] - df['RSI'].rolling(window=14).min()) /
            (df['RSI'].rolling(window=14).max() - df['RSI'].rolling(window=14).min())) * 100
        df['StochRSI_K_Smoothed'] = df['StochRSI_K'].rolling(window=3).mean()
        df['StochRSI_D'] = df['StochRSI_K_Smoothed'].rolling(window=3).mean()
        df.dropna(inplace=True)

        df['EntryLong'] = np.where(
            (df['SupertrendColourOne'] == 1) &
            (df['SupertrendColourTwo'] == 1) &
            (df['SupertrendColourThree'] == 1) &
            (df['StochRSI_K_Smoothed'] > 80),
            "EntryLong", "")

        df['ExitLong'] = np.where(df['SupertrendColourTwo'] == -1, "ExitLong", "")

        df = df[df.index > startTimeEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{stockName}_df.csv")

        amountPerTrade = 100000
        lastIndexTimeData = None

        for timeData in df.index:
            stockAlgoLogic.timeData = timeData
            stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)

            if lastIndexTimeData in df.index:
                logger.info(f"Datetime: {stockAlgoLogic.humanTime}\tStock: {stockName}\tClose: {df.at[lastIndexTimeData,'c']}")

            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    try:
                        stockAlgoLogic.openPnl.at[index, 'CurrentPrice'] = df.at[lastIndexTimeData, "c"]
                    except Exception as e:
                        logging.info(e)
            stockAlgoLogic.pnlCalculator()

            for index, row in stockAlgoLogic.openPnl.iterrows():
                if lastIndexTimeData in df.index:

                    if row['PositionStatus'] == 1:

                        if df.at[lastIndexTimeData, "ExitLong"] == "ExitLong":
                            exitType = "ExitUsingSupertrend"
                            stockAlgoLogic.exitOrder(index, exitType, df.at[lastIndexTimeData, "c"])

            if (lastIndexTimeData in df.index) & (stockAlgoLogic.openPnl.empty) & (stockAlgoLogic.humanTime.time() < time(15, 15)):

                if (df.at[lastIndexTimeData, "EntryLong"] == "EntryLong"):
                    entry_price = df.at[lastIndexTimeData, "c"]
                    stockAlgoLogic.entryOrder(entry_price, stockName, (amountPerTrade//entry_price), "BUY")

            lastIndexTimeData = timeData
            stockAlgoLogic.pnlCalculator()

        if not stockAlgoLogic.openPnl.empty:
            for index, row in stockAlgoLogic.openPnl.iterrows():
                exitType = "TimeUp"
                stockAlgoLogic.exitOrder(index, exitType)
        stockAlgoLogic.pnlCalculator()

if __name__ == "__main__":
    startNow = datetime.now()

    devName = "NA"
    strategyName = "equityDelta"
    version = "v1"

    startDate = datetime(2019, 1, 1, 9, 15)
    endDate = datetime(2025, 12, 31, 15, 30)

    portfolio = createPortfolio("/root/akashResearchAndDevelopment/stocksList/fno_153.md", 4)
    # portfolio = createPortfolio("/root/akashResearchAndDevelopment/stocksList/nifty50.md", 1)

    algoLogicObj = equityDelta(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    dailyReport = calculate_mtm(closedPnl, fileDir, timeFrame="15T", mtm=False, equityMarket=True)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")