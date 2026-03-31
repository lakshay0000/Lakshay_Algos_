import re
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
        col = ["Target", "stoploss", "Expiry", "Trailing_Target"]  # Add "Trailing_Flag" if needed
        self.addColumnsToOpenPnlDf(col)

        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            # Fetch historical data for backtesting
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            df_1d = getFnoBacktestData(indexSym, startEpoch-(86400*50), endEpoch, "1D")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_1d.dropna(inplace=True)


        df_1d['EMA10'] = df_1d['c'].ewm(span=10, adjust=False).mean()

        # Determine crossover signals
        df_1d["EMADown"] = np.where((df_1d["EMA10"] < df_1d["EMA10"].shift(1)), 1, 0)
        df_1d["EMAUp"] = np.where((df_1d["EMA10"] > df_1d["EMA10"].shift(1)), 1, 0)

        # Add 33360 to the index to match the timestamp
        df_1d.index = df_1d.index + 33300
        df_1d.ti = df_1d.ti + 33300

        df_1d = df_1d[df_1d.index >= (startEpoch-(86400*5))]


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_1d.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1d.csv")
        


        lastIndexTimeData = [0, 0]

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        Perc = 2  
        Expiry_Day = False



        

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
                Expiry_Day = True


            if self.humanTime.time() == time(9, 16):
                open_epoch = lastIndexTimeData[1]


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
                    

                    # Exit conditions for CE and PE legs
                    if self.timeData >= row["Expiry"]:
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)


            # callCounter= self.openPnl['Symbol'].str[-2:].value_counts().get('CE',0)
            # putCounter= self.openPnl['Symbol'].str[-2:].value_counts().get('PE',0)

            

            # Check for entry signals and execute orders
            if ((timeData-60) in df.index):

                if self.openPnl.empty and self.humanTime.date() != expiryDatetime.date() and self.humanTime.time() > time(15, 20):
                    if self.humanTime.time() == time(15, 25) and Expiry_Day == True:
                        Expiry_Day = False
                        if df_1d.at[open_epoch, "EMADown"] == 1:
                            #Straddle Price Calculation
                            callSym = self.getCallSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, strikeDist=100)

                            try:
                                data = self.fetchAndCacheFnoHistData(
                                    callSym, lastIndexTimeData[1])
                            except Exception as e:
                                self.strategyLogger.info(e)

                            
                            self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)

                            Buying_price = df.at[lastIndexTimeData[1], "c"] + data["c"]

                            callSym = self.getCallSym(
                                self.timeData, baseSym, Buying_price, expiry= Currentexpiry, strikeDist=100)
                            
                            try:
                                data = self.fetchAndCacheFnoHistData(
                                    callSym, lastIndexTimeData[1])
                            except Exception as e:
                                self.strategyLogger.info(e)

                            self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch},)
                            
                            
                        elif df_1d.at[open_epoch, "EMAUp"] == 1:
                            #Straddle Price Calculation
                            putSym = self.getPutSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, strikeDist=100)

                            try:
                                data = self.fetchAndCacheFnoHistData(
                                    putSym, lastIndexTimeData[1])
                            except Exception as e:
                                self.strategyLogger.info(e)

                            
                            self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)

                            Buying_price = df.at[lastIndexTimeData[1], "c"] - data["c"]

                            putSym = self.getPutSym(
                                self.timeData, baseSym, Buying_price, expiry= Currentexpiry, strikeDist=100)
                            
                            try:
                                data = self.fetchAndCacheFnoHistData(
                                    putSym, lastIndexTimeData[1])
                            except Exception as e:
                                self.strategyLogger.info(e)

                            self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch},)




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