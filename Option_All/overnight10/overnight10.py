import numpy as np
import talib as ta
import pandas_ta as taa
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getFnoBacktestData

# sys.path.insert(1, '/root/backtestTools')


# Define a class algoLogic that inherits from optOverNightAlgoLogic
class algoLogic(optOverNightAlgoLogic):

    # Define a method to get current expiry epoch
    def getCurrentExpiryEpoch(self, date, baseSym):
        expiryData = getExpiryData(date, baseSym)

        # Select appropriate expiry based on the current date
        expiry = expiryData["CurrentExpiry"]

        expiryDatetime = datetime.strptime(expiry, "%d%b%y")
        expiryDatetime = expiryDatetime.replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()

        return expiryEpoch

    # Define a method to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):

        # Add necessary columns to the DataFrame
        col = ["Target", "Stoploss", "BaseSymStoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            # Fetch historical data for backtesting
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            df_5min = getFnoBacktestData(
                indexSym, startEpoch, endEpoch, "5Min")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_5min.dropna(inplace=True)

        results = []
    # Calculate the 12-period EMA
        df_5min['EMA12'] = df_5min['c'].ewm(span=12, adjust=False).mean()

    # Calculate the 26-period EMA
        df_5min['EMA26'] = df_5min['c'].ewm(span=26, adjust=False).mean()

    # Calculate MACD (the difference between 12-period EMA and 26-period EMA)
        df_5min['MACD'] = df_5min['EMA12'] - df_5min['EMA26'] 
        # Calculate the 9-period EMA of MACD (Signal Line)
        df_5min['Signal_Line'] = df_5min['MACD'].ewm(span=9, adjust=False).mean() 
        df_5min.dropna(inplace=True)


        results = taa.supertrend(df_5min["h"], df_5min["l"], df_5min["c"], length=10, multiplier=3.0)
        print(results)
        df_5min["Supertrend"] = results["SUPERTd_10_3.0"]
        df_5min.dropna(inplace=True)


        df_5min['cross1'] = np.where((df_5min['MACD'] > df_5min['Signal_Line']) & (df_5min['MACD'].shift(1) < df_5min['Signal_Line'].shift(1)),1,0)
        df_5min['cross2'] = np.where((df_5min['MACD'] < df_5min['Signal_Line']) & (df_5min['MACD'].shift(1) > df_5min['Signal_Line'].shift(1)),1,0)


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_5min.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_5Min.csv"
        )

        # Strategy Parameters
        calln = 0
        putn = 0
        lastIndexTimeData = [0, 0]
        last5MinIndexTimeData = [0, 0]

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index:
            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            if (timeData-300) in df_5min.index:
                last5MinIndexTimeData.pop(0)
                last5MinIndexTimeData.append(timeData-300)  
                
            # Reset tradeCounter on new day
            # callTradeCounter = (0 if self.humanTime.date() != datetime.fromtimestamp(
            #     timeData).date() else callTradeCounter)
            # putTradeCounter = (0 if self.humanTime.date() != datetime.fromtimestamp(
            #     timeData).date() else putTradeCounter)

            callTradeCounter = 0
            putTradeCounter = 0

            self.timeData = timeData
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # Skip time periods outside trading hours
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            # Strategy Specific Trading Time
            if (self.humanTime.time() < time(9, 20)) | (self.humanTime.time() > time(15, 25)):
                continue

            # Log relevant information
            if lastIndexTimeData[1] in df.index and last5MinIndexTimeData[1] in df_5min.index:
                self.strategyLogger.info(
                    f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")

            # Update current price for open positions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)

            # Calculate and update PnL
            self.pnlCalculator()

            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]

                    if row["CurrentPrice"] >= row["Target"]:
                        exitType = "Target Hit"
                        self.exitOrder(index, exitType, row["Target"])
                    elif row["CurrentPrice"] <= row["Stoploss"]:
                        exitType = "Stoploss Hit"
                        self.exitOrder(index, exitType, row["Stoploss"])
                    elif self.timeData >= row["Expiry"]:
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)

                    if (index in self.openPnl.index) & (symSide == "CE"):
                        callTradeCounter += 1
                    elif (index in self.openPnl.index) & (symSide == "PE"):
                        putTradeCounter += 1

            # Check for entry signals and execute orders
            if ((timeData-300) in df_5min.index):
                if (callTradeCounter < 3):
                    if df_5min.at[last5MinIndexTimeData[1], "cross1"] == 1 and df_5min.at[last5MinIndexTimeData[1], "Supertrend"] == 1:
                        callSym = self.getCallSym(
                            self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"])
                        expiryEpoch = self.getCurrentExpiryEpoch(
                            self.timeData, baseSym)
                        lotSize = int(getExpiryData(
                            self.timeData, baseSym)["LotSize"])
                
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, last5MinIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 1.7 * data["c"]
                        stoploss = 0.7 * data["c"]

                        self.entryOrder(data["c"], callSym, lotSize, "BUY", {
                                        "Target": target,
                                        "Stoploss": stoploss,
                                        "Expiry": expiryEpoch, }
                                         )
                        

                if (putTradeCounter < 3):
                    if df_5min.at[last5MinIndexTimeData[1], "cross2"] == 1 and df_5min.at[last5MinIndexTimeData[1], "Supertrend"] == -1:
                        putSym = self.getPutSym(
                            self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"])
                        expiryEpoch = self.getCurrentExpiryEpoch(
                            self.timeData, baseSym)
                        lotSize = int(getExpiryData(
                            self.timeData, baseSym)["LotSize"])

                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, last5MinIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 1.7 * data["c"]
                        stoploss = 0.7 * data["c"]

                        self.entryOrder(data["c"], putSym, lotSize, "BUY", {
                                        "Target": target,
                                        "Stoploss": stoploss,
                                        "Expiry": expiryEpoch, },
                                        )



        # Calculate final PnL and combine CSVs
        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    # Define Strategy Nomenclature
    devName = "NA"
    strategyName = "rdx"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2023, 1, 1, 9, 15)
    endDate = datetime(2023, 1, 25, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    # Execute the algorithm
    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    # print("Calculating Daily Pnl")
    # dr = calculateDailyReport(
    #     closedPnl, fileDir, timeFrame=timedelta(minutes=5), mtm=True
    # )

    # limitCapital(closedPnl, fileDir, maxCapitalAmount=1000)

    # generateReportFile(dr, fileDir)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")