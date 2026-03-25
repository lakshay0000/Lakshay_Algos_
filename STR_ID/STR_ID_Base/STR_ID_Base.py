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

    def checkAndExecuteExits(self, side):
        """
        Check open positions for exit conditions (trailing target, stoploss, target, time up).
        Updates stoploss if trailing target is hit and executes exit orders when conditions are met.
        
        Args:
            side: The side (CE or PE) to check for exit conditions
        """
        if self.openPnl.empty:
            return
        
        for index, row in self.openPnl.iterrows():
            symSide = row["Symbol"]
            symSide = symSide[len(symSide) - 2:]  # Extract CE or PE
            
            if symSide == side:
                if row["CurrentPrice"] < row["EntryPrice"]:
                    if symSide == "CE":
                        exitType = "Call Profit Exit"
                    else:
                        exitType = "Put Profit Exit"

                    self.exitOrder(index, exitType)
                    self.strategyLogger.info(f"{self.humanTime} {exitType} for {row['Symbol']}")

                else:
                    self.openPnl.at[index, "Target"] = row["EntryPrice"]
                    self.openPnl.at[index, "stoploss"] = row["EntryPrice"]*2
                    self.strategyLogger.info(f"{self.humanTime} Target moved to Breakeven {row['EntryPrice']} and stoploss moved to {row['EntryPrice']*2} for {row['Symbol']}")

        

    # Define a method to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):

        # Add necessary columns to the DataFrame
        col = ["Target", "stoploss", "Expiry", "Trailing_Target"]  # Add "Trailing_Flag" if needed
        self.addColumnsToOpenPnlDf(col)

        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            # Fetch historical data for backtesting
            df = getFnoBacktestData(indexSym, startEpoch-(86400*50), endEpoch, "1Min")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)

        # Calculate the 20-period EMA
        df['EMA20'] = df['c'].ewm(span=20, adjust=False).mean()

        results=[]
        results = taa.stochrsi(df["c"], length=14, rsi_length=14, k=3, d=3)
        df["%K"] = results["STOCHRSIk_14_14_3_3"]
        df["%D"] = results["STOCHRSId_14_14_3_3"]

        # Filter dataframe from timestamp greater than start time timestamp
        df = df[df.index >= startEpoch]

        # Determine crossover signals
        df["%DCross80"] = np.where((df["%D"] < 80) & (df["%D"].shift(1) >= 80), 1, 0)
        df["%DCross20"] = np.where((df["%D"] > 20) & (df["%D"].shift(1) <= 20), 1, 0)

        # Determine crossover signals
        df["EMADown"] = np.where((df["EMA20"] < df["EMA20"].shift(1)), 1, 0)
        df["EMAUp"] = np.where((df["EMA20"] > df["EMA20"].shift(1)), 1, 0)


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        


        lastIndexTimeData = [0, 0]

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        putentryallowed = False
        callentryallowed = False


        

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # # Skip the dates 2nd March 2024 and 18th May 2024
            # if self.humanTime.date() == datetime(2025, 4, 7).date() or self.humanTime.date() == datetime(2025, 6, 16).date():
            #     continue

            # Skip time periods outside trading hours
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)


            # Strategy Specific Trading Time
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            #  # Log relevant information
            # if lastIndexTimeData[1] in df.index:
            #     self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")


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
            

            if self.humanTime.date() > expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
                putentryallowed = False
                callentryallowed = False


            
            # if not self.openPnl.empty:
            #     Current_strangle_value = self.openPnl['CurrentPrice'].sum()
            #     open_sum = self.openPnl['Pnl'].sum()
            #     pnnl_sum = sum(pnnl) 
            #     self.strategyLogger.info(f"pnl_sum:{open_sum + pnnl_sum}")

            #     if (open_sum + pnnl_sum) <= -6000:
            #         for index, row in self.openPnl.iterrows():
            #             self.exitOrder(index, "MaxLoss")
            #             EntryAllowed = False
            #             pnnl = []
            #             i = 3
            #             i_CanChange = False


            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]  
                    
                    # self.strategyLogger.info(f"{self.openPnl[['Symbol', 'Target', 'stoploss']].to_string()}")

                    if row["CurrentPrice"] <= row["Trailing_Target"]:
                        self.openPnl.at[index, "stoploss"] = row["CurrentPrice"]*2
                        self.openPnl.at[index, "Trailing_Target"] = row["CurrentPrice"]
                        self.strategyLogger.info(f"{self.humanTime} {row['Symbol']} Trailing_Target HIT CE and stoploss shifted to: {self.openPnl.at[index, 'stoploss']}")
                        self.strategyLogger.info(f"Trailing_Target: {self.openPnl.at[index, 'Trailing_Target']}")
                        self.strategyLogger.info(f"{self.openPnl[['Symbol', 'Target', 'stoploss']].to_string()}")
                    
                    # Exit conditions for CE and PE legs
                    if self.humanTime.time() >= time(15, 20):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)

                    elif row["CurrentPrice"] >= row["stoploss"]:
                        exitType = "Stoploss Hit"
                        # strike = row["Symbol"][12:-2]  
                        self.exitOrder(index, exitType)
                               

                    elif row["CurrentPrice"] <= row["Target"]:
                        exitType = "Target Hit"
                        self.exitOrder(index, exitType)


            # if callCounter == 3 or putCounter == 3:
            #     self.strategyLogger.info(f"{self.humanTime} 3 CE or PE positions are open. Current CE count: {callCounter}, Current PE count: {putCounter}")
            #     StraddleEntryAllowed = False

            callCounter= self.openPnl['Symbol'].str[-2:].value_counts().get('CE',0)
            putCounter= self.openPnl['Symbol'].str[-2:].value_counts().get('PE',0)

            

            # Check for entry signals and execute orders
            if ((timeData-60) in df.index):

                if self.humanTime.date() == expiryDatetime.date() and self.humanTime.time() < time(15, 20):

                    if df.at[lastIndexTimeData[1], "%DCross80"] == 1:
                        callentryallowed = True
                    if df.at[lastIndexTimeData[1], "%DCross20"] == 1:
                        putentryallowed = True


                    if (putentryallowed) and putCounter < 3: 
                        if df.at[lastIndexTimeData[1], "EMAUp"] == 1:
                            putSym = self.getPutSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)

                            try:
                                data = self.fetchAndCacheFnoHistData(
                                    putSym, lastIndexTimeData[1])
                            except Exception as e:
                                self.strategyLogger.info(e)

                            stoploss = 1.3 * data["c"]
                            target = 0.1 * data["c"]
                            trailingTarget = 0.5 * data["c"]

                            self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Target": target, "stoploss": stoploss, "Trailing_Target": trailingTarget},)
                            putentryallowed = False  
                            self.checkAndExecuteExits("CE")  # Check for CE exit conditions after entering a PE position
                    
                    if (callentryallowed) and callCounter < 3:
                        if df.at[lastIndexTimeData[1], "EMADown"] == 1:
                            callSym = self.getCallSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)

                            try:
                                data = self.fetchAndCacheFnoHistData(
                                    callSym, lastIndexTimeData[1])
                            except Exception as e:
                                self.strategyLogger.info(e)

                            stoploss = 1.3 * data["c"]
                            target = 0.1 * data["c"]
                            trailingTarget = 0.5 * data["c"]

                            self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Target": target, "stoploss": stoploss, "Trailing_Target": trailingTarget},)
                            callentryallowed = False  
                            self.checkAndExecuteExits("PE")  # Check for PE exit conditions after entering a CE position



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
    endDate = datetime(2025, 12, 31, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    # Execute the algorithm
    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    # dr = calculateDailyReport(
    #     closedPnl, fileDir, timeFrame=timedelta(minutes=5), mtm=True
    # )

    # limitCapital(closedPnl, fileDir, maxCapitalAmount=1000)

    # generateReportFile(dr, fileDir)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")