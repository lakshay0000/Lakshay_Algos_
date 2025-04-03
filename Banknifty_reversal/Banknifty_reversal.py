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
            df_1h = getFnoBacktestData(
                indexSym, startEpoch-(86400*50), endEpoch, "1H")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_1h.dropna(inplace=True)


        #calculating bollinger bands
        # Parameters
        window = 20  # Window size for moving average
        std_multiplier = 2  # Number of standard deviations

        # Step 1: Calculate the Middle Band (SMA)
        df_1h["Middle Band"] = df_1h["c"].rolling(window=window).mean()

        # Step 2: Calculate the standard deviation
        df_1h["Std Dev"] = df_1h["c"].rolling(window=window).std()

        # Step 3: Calculate Upper and Lower Bands
        df_1h["Upper Band"] = df_1h["Middle Band"] + (std_multiplier * df_1h["Std Dev"])
        df_1h["Lower Band"] = df_1h["Middle Band"] - (std_multiplier * df_1h["Std Dev"])

        # Drop rows with NaN values (caused by rolling calculations)
        df_1h.dropna(inplace=True)
        
        results=[]
        results = taa.stochrsi(df_1h["c"], length=14, rsi_length=14, k=3, d=3)
        df_1h["%K"] = results["STOCHRSIk_14_14_3_3"]
        df_1h["%D"] = results["STOCHRSId_14_14_3_3"]
        

        # Filter dataframe from timestamp greater than start time timestamp
        df_1h = df_1h[df_1h.index >= startEpoch]

        # Determine crossover signals
        df_1h["Brcross"]= np.where((df_1h["c"] < df_1h["Upper Band"]) & (df_1h["c"].shift(1) > df_1h["Upper Band"].shift(1)), 1, 0)
        df_1h["Bucross"] = np.where((df_1h["c"] > df_1h["Lower Band"]) & (df_1h["c"].shift(1) < df_1h["Lower Band"].shift(1)), 1, 0)
        df_1h["Midcross"] = np.where((df_1h["c"] >= df_1h["Middle Band"]) & (df_1h["c"] < df_1h["Middle Band"]).shift(1), 1, np.where((df_1h["c"] <= df_1h["Middle Band"]) & (df_1h["c"] > df_1h["Middle Band"]).shift(1), 1, 0),)


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_1h.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1H.csv"
        )

        # Strategy Parameters       
        lastIndexTimeData = [0, 0]
        last5MinIndexTimeData = [0, 0]
        stoploss = False


        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # Skip the dates 2nd March 2024 and 18th May 2024
            if self.humanTime.date() in [datetime(2024, 3, 2).date(), datetime(2024, 5, 18).date()]:
                continue

            # Skip time periods outside trading hours
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            if (timeData-3600) in df_1h.index:
                last5MinIndexTimeData.pop(0)
                last5MinIndexTimeData.append(timeData-3600)  

            # Strategy Specific Trading Time
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            # # Log relevant information
            # if (timeData-300) in df_1h.index:
            #     self.strategyLogger.info(
            #         f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}\trsi60: {df_1h.at[last5MinIndexTimeData[1],'rsiCross60']}\trsi50: {df_1h.at[last5MinIndexTimeData[1],'rsiCross50']}\trsi40: {df_1h.at[last5MinIndexTimeData[1],'rsiCross40']}")

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

            if self.humanTime.date() > expiryDatetime.date() :
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']   
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()             


            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]
      
                    if (df_1h.at[last5MinIndexTimeData[1], "Midcross"] == 1):
                        exitType = "Target Hit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif row["CurrentPrice"] >= row["Stoploss"] and row["PositionStatus"] == -1:
                        stoploss= True

                    elif row["CurrentPrice"] <= row["Stoploss"] and row["PositionStatus"] == 1:
                        stoploss= True

                    elif self.timeData >= row["Expiry"]:
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)

            if stoploss== True:
                for index, row in self.openPnl.iterrows():
                    self.exitOrder(index, "STOPLOSS HIT")
                    stoploss= False

    
            # Check for entry signals and execute orders
            if ((timeData-3600) in df_1h.index):
                
                if (df_1h.at[last5MinIndexTimeData[1], "Bucross"] == 1) & (df_1h.at[last5MinIndexTimeData[1], "%K"] < 8):


                    if (self.humanTime.date()== expiryDatetime.date()):

                        callSym = self.getCallSym(self.timeData+86400, baseSym, df_1h.at[last5MinIndexTimeData[1], "c"])
                    else:
                        callSym = self.getCallSym(self.timeData, baseSym, df_1h.at[last5MinIndexTimeData[1], "c"])

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    stoploss = 0.7 * data["c"]

                    self.entryOrder(data["c"], callSym, lotSize, "BUY", {
                                    "Stoploss": stoploss,
                                    "Expiry": expiryEpoch, }
                                    )
                    
                    if (self.humanTime.date()== expiryDatetime.date()):

                        callSym = self.getCallSym(self.timeData+86400, baseSym, df_1h.at[last5MinIndexTimeData[1], "c"],otmFactor=5)
                    else:
                        callSym = self.getCallSym(self.timeData, baseSym, df_1h.at[last5MinIndexTimeData[1], "c"],otmFactor=5)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    stoploss = 1.3 * data["c"]

                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {
                                    "Stoploss": stoploss,
                                    "Expiry": expiryEpoch, }
                                    )
                

                if (df_1h.at[last5MinIndexTimeData[1], "Brcross"] == 1) & (df_1h.at[last5MinIndexTimeData[1], "%K"] > 90):

                    if (self.humanTime.date()== expiryDatetime.date()):

                        putSym = self.getPutSym(self.timeData+86400, baseSym, df_1h.at[last5MinIndexTimeData[1], "c"])
                    else:
                        putSym = self.getPutSym(self.timeData, baseSym, df_1h.at[last5MinIndexTimeData[1], "c"])

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    stoploss = 0.7 * data["c"]

                    self.entryOrder(data["c"], putSym, lotSize, "BUY", {
                                    "Stoploss": stoploss,
                                    "Expiry": expiryEpoch, },
                                    )
                    
                    if (self.humanTime.date()== expiryDatetime.date()):

                        putSym = self.getPutSym(self.timeData+86400, baseSym, df_1h.at[last5MinIndexTimeData[1], "c"],otmFactor=5)
                    else:
                        putSym = self.getPutSym(self.timeData, baseSym, df_1h.at[last5MinIndexTimeData[1], "c"],otmFactor=5)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e) 

                    stoploss = 1.3 * data["c"]

                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {
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
    endDate = datetime(2025, 3, 20, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "BANKNIFTY"
    indexName = "NIFTY BANK"

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