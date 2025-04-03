import multiprocessing as mp
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
            df_1d = getFnoBacktestData(indexSym, startEpoch-(86400*10), endEpoch, "1D")

            # Skip holidays
            if df is None or df_1d is None:
                return
            
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_1d.dropna(inplace=True)
        
        # Add 33360 to the index to match the timestamp
        df_1d.index = df_1d.index + 33360
        df_1d.ti = df_1d.ti + 33360

        # Filter dataframe from timestamp greater than start time timestamp
        df_1d = df_1d[df_1d.index >= startEpoch-86340]


        # Save the dataframe to a CSV file
        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_{startDate.date()}_1Min.csv")
        df_1d.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_{startDate.date()}_1Day.csv")

        # Get lot size from expiry data
        # lotSize = int(getExpiryData(startEpoch, baseSym)["LotSize"])
        lotSize = int(self.fetchAndCacheExpiryData(
            startEpoch, baseSym)["LotSize"])

        # Strategy Parameters
        lastIndexTimeData = [0, 0]
        last1DIndexTimeData = [0, 0]
        NextTradeEntry=False
        prevDayHigh = None
        prevDayLow = None

        # Loop through each timestamp in the dataframe index
        for timeData in df.index:
            self.timeData = timeData
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # # Skip time periods outside trading hours
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue


            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData - 60)
            if timeData-86400 in df_1d.index:
                last1DIndexTimeData.pop(0)
                last1DIndexTimeData.append(timeData-86400)


            # Add self.strategyLogger and comments
            if lastIndexTimeData[1] in df.index:
                self.strategyLogger.info(
                    f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")
                
            self.strategyLogger.info(f"1Depoch: {last1DIndexTimeData}")               

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

            if last1DIndexTimeData[1] in df_1d.index:
                prevDayHigh = df_1d.at[last1DIndexTimeData[1], 'h']
                prevDayLow = df_1d.at[last1DIndexTimeData[1], 'l']
                self.strategyLogger.info(f"Prev Day High: {prevDayHigh}")
                self.strategyLogger.info(f"Prev Day Low: {prevDayLow}")


            # Exit positions based on conditions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]

                    if row["CurrentPrice"] <= (row["EntryPrice"] - (0.5 * row["EntryPrice"])) and row["PositionStatus"] == -1:
                        exitType = " Target Hit"
                        self.exitOrder(index, exitType)
                    elif row["CurrentPrice"] >= (row["EntryPrice"] + (0.3 * row["EntryPrice"])) and row["PositionStatus"] == -1:
                        exitType = "Stoploss Hit"
                        self.exitOrder(index, exitType)                                             
                    elif self.humanTime.time() >= time(15, 15):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)
                    elif (df.at[lastIndexTimeData[1], "c"] > prevDayHigh) & (symSide == "CE"):  
                            exitType = "Candle Stoploss Hit"
                            self.exitOrder(index, exitType)
                    elif (df.at[lastIndexTimeData[1], "c"] < prevDayLow) & (symSide == "PE"):
                            exitType = "Candle Stoploss Hit"
                            self.exitOrder(index, exitType)


            # Place orders based on conditions
            if (lastIndexTimeData[1] in df.index) & (self.humanTime.time() == time(9, 16)): 
                if prevDayLow is not None and prevDayHigh is not None:
                    if prevDayLow <= (df.at[lastIndexTimeData[1],"c"]) <= prevDayHigh:
                        callSym = self.getCallSym(startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"],otmFactor= 0)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        self.entryOrder(data["c"], callSym, lotSize, "SELL")

                        putSym = self.getPutSym(startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"],otmFactor= 0)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        self.entryOrder(data["c"], putSym, lotSize, "SELL")
                        NextTradeEntry=True
                        # self.strategyLogger.info(f"NextTradeEntry: {NextTradeEntry}")

                    if (df.at[lastIndexTimeData[1],"c"]) > prevDayHigh: 
                        putSym = self.getPutSym(
                            startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor= 0)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        self.entryOrder(data["c"], putSym, lotSize, "BUY")
                        NextTradeEntry=True

                    if (df.at[lastIndexTimeData[1],"c"]) < prevDayLow:
                        callSym = self.getCallSym(startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"],otmFactor= 0)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        self.entryOrder(data["c"], callSym, lotSize, "SELL")
                        NextTradeEntry=True

            if NextTradeEntry:
                if (df.at[lastIndexTimeData[1],"c"]) > prevDayHigh: 
                    putSym = self.getPutSym(
                        startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor= 0)
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "BUY")
                    NextTradeEntry=False

                if (df.at[lastIndexTimeData[1],"c"]) < prevDayLow:
                    callSym = self.getCallSym(startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"],otmFactor= 0)
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "SELL")
                    NextTradeEntry=False

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
    startDate = datetime(2023, 1, 5, 9, 15)
    endDate = datetime(2023, 1, 31, 15, 30)

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