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

    # Define a method to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):

        # Add necessary columns to the DataFrame
        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            # Fetch historical data for backtesting
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            df_5min = getFnoBacktestData(
                indexSym, startEpoch-886400, endEpoch, "5Min")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_5min.dropna(inplace=True)


        # Calculate RSI indicator
        df_5min["rsi"] = ta.RSI(df_5min["c"], timeperiod=14)
        df_5min.dropna(inplace=True)

        results=[]
        results = taa.supertrend(df_5min["h"], df_5min["l"], df_5min["c"], length=10, multiplier=3.0)
        # print(results)
        df_5min["Supertrend"] = results["SUPERTd_10_3.0"]
        df_5min.dropna(inplace=True)

        # Filter dataframe from timestamp greater than start time timestamp
        df_5min = df_5min[df_5min.index > startEpoch]

        # Determine crossover signals
        df_5min["rsiCross60"] = np.where(
            (df_5min["rsi"] > 60) & (df_5min["rsi"].shift(1) <= 60), 1, 0)
        df_5min["rsiCross40"] = np.where(
            (df_5min["rsi"] < 40) & (df_5min["rsi"].shift(1) >= 40), 1, 0)
        df_5min["rsiCross50"] = np.where((df_5min["rsi"] >= 50) & (df_5min["rsi"].shift(
            1) < 50), 1, np.where((df_5min["rsi"] <= 50) & (df_5min["rsi"].shift(1) > 50), 1, 0),)

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_5min.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_5Min.csv"
        )

        # Strategy Parameters
        callEntryAllow = True
        putEntryAllow = True        
        lastIndexTimeData = [0, 0]
        last5MinIndexTimeData = [0, 0]


        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)


            # Skip time periods outside trading hours
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            if (timeData-300) in df_5min.index:
                last5MinIndexTimeData.pop(0)
                last5MinIndexTimeData.append(timeData-300)

            # Strategy Specific Trading Time
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            # Log relevant information
            if (timeData-300) in df_5min.index:
                self.strategyLogger.info(
                    f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}\trsi60: {df_5min.at[last5MinIndexTimeData[1],'rsiCross60']}\trsi50: {df_5min.at[last5MinIndexTimeData[1],'rsiCross50']}\trsi40: {df_5min.at[last5MinIndexTimeData[1],'rsiCross40']}")

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

            if self.humanTime.date() == expiryDatetime.date() :
                Currentexpiry = getExpiryData(self.timeData+86400, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
                

            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]
      
                    if row["CurrentPrice"] <= row["Target"]:
                        exitType = "Target Hit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif row["CurrentPrice"] >= row["Stoploss"]:
                        exitType = "Stoploss Hit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif self.timeData >= row["Expiry"]:
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)
    

            tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            callCounter= tradecount.get('CE',0)
            putCounter= tradecount.get('PE',0)

            # Check for entry signals and execute orders
            if ((timeData-300) in df_5min.index):
                if df_5min.at[last5MinIndexTimeData[1], "rsiCross50"] == 1:
                    callEntryAllow = True
                    putEntryAllow = True
                
                if (callCounter < 3) and (callEntryAllow):
                    if df_5min.at[last5MinIndexTimeData[1], "rsiCross40"] == 1 and df_5min.at[last5MinIndexTimeData[1], "Supertrend"] ==-1:
                        callSym = self.getCallSym(
                            self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"],expiry= Currentexpiry)

                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, last5MinIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 0.3 * data["c"]
                        stoploss = 1.3 * data["c"]

                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {
                                        "Target": target,
                                        "Stoploss": stoploss,
                                        "Expiry": expiryEpoch, }
                                        )
                        callEntryAllow = False

                if (putCounter < 3) and (putEntryAllow):       
                    if df_5min.at[last5MinIndexTimeData[1], "rsiCross60"] == 1 and df_5min.at[last5MinIndexTimeData[1], "Supertrend"] ==1:
                        putSym = self.getPutSym(
                            self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"],expiry= Currentexpiry)

                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 0.3 * data["c"]
                        stoploss = 1.3 * data["c"]

                        self.entryOrder(data["c"], putSym, 5*lotSize, "SELL", {
                                        "Target": target,
                                        "Stoploss": stoploss,
                                        "Expiry": expiryEpoch, },
                                        )
                        putEntryAllow = False



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

    print("Calculating Daily Pnl")
    dr = calculateDailyReport(
        closedPnl, fileDir, timeFrame=timedelta(minutes=5), mtm=True
    )

    limitCapital(closedPnl, fileDir, maxCapitalAmount=1000)

    generateReportFile(dr, fileDir)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")