import multiprocessing as mp
import numpy as np
import talib as ta
import pandas_ta as taa
from datetime import datetime, timedelta, time
from backtestTools.algoLogic import optIntraDayAlgoLogic
from backtestTools.histData import getFnoBacktestData


# Define a class algoLogic that inherits from optIntraDayAlgoLogic
class algoLogic(optIntraDayAlgoLogic):

    # Define a method 'run' to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):
        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            # Fetch historical data for backtesting  
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            df_5min = getFnoBacktestData(indexSym, startEpoch- 864000, endEpoch, "5Min")

            # Skip holidays
            if df is None:
                return
            if df_5min is None:
                return
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)
        
        results = []
        MACD =[]
        Signal_Line =[]
        MACD_Histogram = []


        # Calculate MACD using pandas-ta
        MACD = taa.macd(
            df_5min['c'], 
            fast=12, 
            slow=26, 
            signal=9
        )
        # print(MACD)
        df_5min["MACD"] = MACD["MACD_12_26_9"]
        df_5min["Signal_Line"] = MACD["MACDs_12_26_9"]


        results = taa.supertrend(df_5min["h"], df_5min["l"], df_5min["c"], length=10, multiplier=3.0)
        # print(results)
        df_5min["Supertrend"] = results["SUPERTd_10_3.0"]
        df_5min.dropna(inplace=True)


        df_5min['cross1'] = np.where((df_5min['MACD'] > df_5min['Signal_Line']) & (df_5min['MACD'].shift(1) < df_5min['Signal_Line'].shift(1)),1,0)
        df_5min['cross2'] = np.where((df_5min['MACD'] < df_5min['Signal_Line']) & (df_5min['MACD'].shift(1) > df_5min['Signal_Line'].shift(1)),1,0)

        df_5min = df_5min[df_5min.index >= startEpoch]
        

        # Save the dataframe to a CSV file
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_{startDate.date()}_1Min.csv")
        df_5min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_{startDate.date()}_5Min.csv")

        # Get lot size from expiry data
        # lotSize = int(getExpiryData(startEpoch, baseSym)["LotSize"])
        lotSize = int(self.fetchAndCacheExpiryData(
            startEpoch, baseSym)["LotSize"])

        # Strategy Parameters
        lastIndexTimeData = [0, 0]
        last5MinIndexTimeData =[0, 0]


        # Loop through each timestamp in the dataframe index
        for timeData in df.index:
        # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData - 60)
            if (timeData-300) in df_5min.index:
                last5MinIndexTimeData.pop(0)
                last5MinIndexTimeData.append(timeData-300)


            self.timeData = timeData
            self.humanTime = datetime.fromtimestamp(timeData)
            # print(self.humanTime)

            # Skip time periods outside trading hours
            if (self.humanTime.time() < time(9, 16)) | (
                self.humanTime.time() > time(15, 30)
            ):
                continue


            # Add self.strategyLogger and comments
            if lastIndexTimeData[1] in df.index:
                self.strategyLogger.info(
                    f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")
                # print(self.humanTime)

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

            # Exit positions based on conditions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    if row["CurrentPrice"] <= (row["EntryPrice"] - (0.7 * row["EntryPrice"])) and row["PositionStatus"] == -1:
                        exitType = " Target Hit"
                        self.exitOrder(index, exitType)
                    elif row["CurrentPrice"] >= (row["EntryPrice"] + (0.3 * row["EntryPrice"])) and row["PositionStatus"] == -1:
                        exitType = "Stoploss Hit"
                        self.exitOrder(index, exitType)
                    elif row["CurrentPrice"] >= (row["EntryPrice"] + (0.7 * row["EntryPrice"])) and row["PositionStatus"] == 1:
                        exitType = " Target Hit"
                        self.exitOrder(index, exitType)  
                    elif row["CurrentPrice"] <= (row["EntryPrice"] - (0.3 * row["EntryPrice"])) and row["PositionStatus"] == 1:
                        exitType = "Stoploss Hit"
                        self.exitOrder(index, exitType)                                              
                    elif self.humanTime.time() >= time(15, 15):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)


            # Place orders based on conditions
            if ((timeData-300) in df_5min.index) & (self.humanTime.time() < time(15, 15)) : 
                if df_5min.at[last5MinIndexTimeData[1], "cross1"] == 1 and df_5min.at[last5MinIndexTimeData[1], "Supertrend"] == 1:
                    callSym = self.getCallSym(
                        startEpoch, baseSym, df.at[last5MinIndexTimeData[1], "c"])

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, last5MinIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "BUY")


                if df_5min.at[last5MinIndexTimeData[1], "cross2"] == 1 and df_5min.at[last5MinIndexTimeData[1], "Supertrend"] == -1:
                    putSym = self.getPutSym(
                        startEpoch, baseSym, df.at[last5MinIndexTimeData[1], "c"])

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, last5MinIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "BUY")
                    

        # Final PnL calculation and CSV export
        self.pnlCalculator()
        self.combinePnlCsv()


if __name__ == "__main__":
    start = datetime.now()

    # Define Strategy Nomenclature
    devName = "NA"
    strategyName = "straddle"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2024, 3, 31, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    # Configure number of processes to be created
    maxConcurrentProcesses = 2
    processes = []

    # Start a loop from Start Date to End Date
    currentDate = startDate
    while currentDate <= endDate:
        # Define trading period for Current day
        startTime = datetime(
            currentDate.year, currentDate.month, currentDate.day, 9, 15, 0)
        endTime = datetime(
            currentDate.year, currentDate.month, currentDate.day, 15, 30, 0)

        p = mp.Process(target=algo.run, args=(
            startTime, endTime, baseSym, indexName))
        p.start()
        processes.append(p)

        if len(processes) >= maxConcurrentProcesses:
            for p in processes:
                p.join()
            processes = []

        currentDate += timedelta(days=1)

    end = datetime.now()
    print(f"Done. Ended in {end-start}.")