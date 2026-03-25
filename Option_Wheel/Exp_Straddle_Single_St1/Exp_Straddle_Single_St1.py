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

    def straddle(self, date, IndexPrice, baseSym):     
        Factor = [0, 1, -1, 2, -2]
        minlist = []
        callSymList = []
        putSymList = []

        for i in Factor:
            callSym = self.getCallSym(self.timeData, baseSym, IndexPrice, otmFactor=i)
            try:
                dataCE = self.fetchAndCacheFnoHistData(callSym, date)
            except Exception as e:
                self.strategyLogger.info(e)
                continue  # Skip this iteration if call data can't be fetched

            if i != 0:
                i_put = -i  # Ensure otmFactor is positive for put options
            else:
                i_put = 0

            putSym = self.getPutSym(self.timeData, baseSym, IndexPrice, otmFactor=i_put)
            try:
                dataPE = self.fetchAndCacheFnoHistData(putSym, date)
            except Exception as e:
                self.strategyLogger.info(e)
                continue  # Skip this iteration if put data can't be fetched

            Diff = abs(dataCE["c"] - dataPE["c"])
            minlist.append(Diff)
            callSymList.append(callSym)
            putSymList.append(putSym)


        # Find the index of the minimum difference
        if minlist:  # Check if minlist is not empty
            self.strategyLogger.info(f"Straddle Option Pairs: {callSymList}")
            self.strategyLogger.info(f"Straddle Option Pairs: {putSymList}")
            self.strategyLogger.info(f"Straddle Option Premium Differences: {minlist}")

            minIndex = np.argmin(minlist)
            return callSymList[minIndex], putSymList[minIndex] 
        else:
            return None  # Return None if no valid pairs found
        

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
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        


        lastIndexTimeData = [0, 0]

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        StraddleEntryAllowed = False
        MainTradeAllowed = True



        

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
                StraddleEntryAllowed = False
                MainTradeAllowed = True


            
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
                    symstrike = row["Symbol"]
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

                        
                        if row["First_Trade"] == True:
                            StraddleEntryAllowed = True

                    elif row["CurrentPrice"] <= row["Target"]:
                        exitType = "Target Hit"
                        self.exitOrder(index, exitType)

                        
                        

            # Check for entry signals and execute orders
            if ((timeData-60) in df.index):

                if self.humanTime.date() == expiryDatetime.date() and self.humanTime.time() < time(15, 20):

                    if MainTradeAllowed:
                        callSym, putSym = self.straddle(lastIndexTimeData[1], df.at[lastIndexTimeData[1], "c"], baseSym)
                    
                        # CE Leg
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        stoploss = 1.3 * data["c"]
                        target = 0.1 * data["c"]
                        trailingTarget = 0.5 * data["c"]

                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "stoploss": stoploss, "Target": target, "Trailing_Target": trailingTarget, "First_Trade": True},)
                        
                        # PE Leg
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        stoploss = 1.3 * data["c"]
                        target = 0.1 * data["c"]
                        trailingTarget = 0.5 * data["c"]

                        
                        self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "stoploss": stoploss, "Target": target, "Trailing_Target": trailingTarget, "First_Trade": True},)

                        MainTradeAllowed = False


                    if StraddleEntryAllowed:

                        callSym, putSym = self.straddle(lastIndexTimeData[1], df.at[lastIndexTimeData[1], "c"], baseSym)
                    
                        # CE Leg
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        stoploss = 1.3 * data["c"]
                        target = 0.1 * data["c"]
                        trailingTarget = 0.5 * data["c"]

                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "stoploss": stoploss, "Target": target, "Trailing_Target": trailingTarget, "First_Trade": False},)
                        
                        # PE Leg
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        stoploss = 1.3 * data["c"]
                        target = 0.1 * data["c"]
                        trailingTarget = 0.5 * data["c"]

                        
                        self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "stoploss": stoploss, "Target": target, "Trailing_Target": trailingTarget, "First_Trade": False},)

                        StraddleEntryAllowed = False



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