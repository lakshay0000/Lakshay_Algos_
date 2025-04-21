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
        # Fetch expiry data for current and next expiry
        expiryData = getExpiryData(date, baseSym)

        # Select appropriate expiry based on the current date
        expiry = expiryData["CurrentExpiry"]

        # Set expiry time to 15:20 and convert to epoch
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
            df_1h = getFnoBacktestData(
                indexSym, startEpoch, endEpoch, "1H")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_1h.dropna(inplace=True)

        df_1h['ema15'] = df_1h['c'].ewm(span=9, adjust=False).mean()
        df_1h.dropna(inplace=True)

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_1h.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1H.csv"
        )

        # Strategy Parameters

        lastIndexTimeData = [0, 0]
        last1HIndexTimeData = [0, 0]

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index:
            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            if (timeData-3600) in df_1h.index:
                last1HIndexTimeData.pop(0)
                last1HIndexTimeData.append(timeData-3600)  
                
            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # Skip time periods outside trading hours
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            # Strategy Specific Trading Time
            if (self.humanTime.time() < time(9, 20)) | (self.humanTime.time() > time(15, 25)):
                continue

            # Log relevant information
            if lastIndexTimeData[1] in df.index and last1HIndexTimeData[1] in df_1h.index:
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

                    if row["CurrentPrice"] <= row["Target"]:
                        exitType = "Target Hit"
                        self.exitOrder(index, exitType, row["Target"])
                    elif row["CurrentPrice"] >= row["Stoploss"]:
                        exitType = "Stoploss Hit"
                        self.exitOrder(index, exitType, row["Stoploss"])
                    elif self.timeData >= row["Expiry"]:
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)


            # Check for entry signals and execute orders
            if ((timeData-3600) in df_1h.index) and (self.humanTime.time() < time(15, 30)):
                    
                if df_1h.at[last1HIndexTimeData[1], "c"] < df_1h.at[last1HIndexTimeData[1], "ema15"] :
                    callSym = self.getCallSym(
                        self.timeData, baseSym, df_1h.at[last1HIndexTimeData[1], "c"])
                    expiryEpoch = self.getCurrentExpiryEpoch(
                        self.timeData, baseSym)
                    lotSize = int(getExpiryData(
                        self.timeData, baseSym)["LotSize"])  

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, last1HIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    target = 0.3 * data["c"]
                    stoploss = 1.3 * data["c"]

                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {
                                    "Target": target,
                                    "Stoploss": stoploss,
                                    "Expiry": expiryEpoch, }
                                    )
                       

                if df_1h.at[last1HIndexTimeData[1], "c"] > df_1h.at[last1HIndexTimeData[1], "ema15"]:
                    putSym = self.getPutSym(
                        self.timeData, baseSym, df_1h.at[last1HIndexTimeData[1], "c"])
                    expiryEpoch = self.getCurrentExpiryEpoch(
                        self.timeData, baseSym)
                    lotSize = int(getExpiryData(
                        self.timeData, baseSym)["LotSize"])

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, last1HIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    target = 0.3 * data["c"]    
                    stoploss = 1.3 * data["c"]



                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {
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
    strategyName = "overnight6"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2023, 1, 1, 9, 15)
    endDate = datetime(2023, 2, 25, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    # Execute the algorithm
    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculateDailyReport(
        closedPnl, fileDir, timeFrame=timedelta(minutes=5), mtm=True
    )

    limitCapital(closedPnl, fileDir, maxCapitalAmount=1000)

    generateReportFile(dr, fileDir)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")