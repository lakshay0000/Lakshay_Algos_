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
        col = ["Expiry"]
        self.addColumnsToOpenPnlDf(col)

        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            # Fetch historical data for backtesting
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            df_5min = getFnoBacktestData(
                indexSym, startEpoch-864000, endEpoch, "5Min")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_5min.dropna(inplace=True)

        df_5min['ema15'] = df_5min['c'].ewm(span=15, adjust=False).mean()
        df_5min.dropna(inplace=True)

        df_5min["emacross1"] = np.where((df_5min["c"] <= df_5min["ema15"]) & (df_5min["c"].shift(1) > df_5min["ema15"].shift(1)), 1, 0)
        df_5min["emacross2"] = np.where((df_5min["c"] >= df_5min["ema15"]) & (df_5min["c"].shift(1) < df_5min["ema15"].shift(1)), 1, 0)

        df_5min = df_5min[df_5min.index > startEpoch]


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_5min.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_5Min.csv")

        # Strategy Parameters

        lastIndexTimeData = [0, 0]
        last5MinIndexTimeData = [0, 0]
        f1=0
        t1=0

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index:
            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            if (timeData-300) in df_5min.index:
                last5MinIndexTimeData.pop(0)
                last5MinIndexTimeData.append(timeData-300)  
                
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
            if lastIndexTimeData[1] in df.index and last5MinIndexTimeData[1] in df_5min.index:
                self.strategyLogger.info(
                    f"Datetime: {self.humanTime}\ttradetime: {last5MinIndexTimeData[1]}\tClose: {df.at[last5MinIndexTimeData[1],'c']}")

            # Update current price for open positions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)
            
            if (timeData-300) in df_5min.index:

                if (df_5min.at[last5MinIndexTimeData[1], "emacross1"]== 1) and (f1==0):
                    f1=1

            if (timeData-300) in df_5min.index:
                if (df_5min.at[last5MinIndexTimeData[1], "emacross2"]== 1) and (t1==0):
                    t1=1


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


            # Check for entry signals and execute orders
            if ((timeData-300) in df_5min.index):

                if (df_5min.at[last5MinIndexTimeData[1], "c"] < df_5min.at[last5MinIndexTimeData[1], "ema15"]) and (t1==1):
                    putSym = self.getPutSym(
                        self.timeData, baseSym, df.at[last5MinIndexTimeData[1], "c"])
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
                    t1=0
                        

                       
                if (df_5min.at[last5MinIndexTimeData[1], "c"] > df_5min.at[last5MinIndexTimeData[1], "ema15"]) and (f1==1):
                    callSym = self.getCallSym(
                        self.timeData, baseSym, df.at[last5MinIndexTimeData[1], "c"])
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
                                    "Expiry": expiryEpoch, },
                                    )
                    f1=0





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
    endDate = datetime(2023, 3, 25, 15, 30)

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
