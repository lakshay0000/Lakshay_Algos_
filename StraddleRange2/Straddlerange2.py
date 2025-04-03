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


    # def round_to_nearest(self, number, distance):
    #     a= round(number / distance) * distance
    #     return a  


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
            df_1d = getFnoBacktestData(indexSym, startEpoch-(86400*10), endEpoch, "1D")
            
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


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_1d.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_5Min.csv"
        )

        # Get lot size from expiry data
        # lotSize = int(getExpiryData(startEpoch, baseSym)["LotSize"])
        lotSize = int(self.fetchAndCacheExpiryData(
            startEpoch, baseSym)["LotSize"])

        # Strategy Parameters
        lastIndexTimeData = [0, 0]
        last1DIndexTimeData = [0, 0]
        CallTradeEntry=False
        PutTradeEntry=False
        prevDayHigh = None
        prevDayLow = None


        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

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

            #Updating daily index
            prev_day = timeData - 86400
            if timeData in df_1d.index:
                #check if previoud day exists in 1d data
                while prev_day not in df_1d.index:
                    prev_day = prev_day - 86400

            if prev_day in df_1d.index:
                last1DIndexTimeData.pop(0)
                last1DIndexTimeData.append(prev_day)


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

            if self.humanTime.date() == expiryDatetime.date() :
                Currentexpiry = getExpiryData(self.timeData+86400, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp() 

            if last1DIndexTimeData[1] in df_1d.index:
                prevDayHigh = df_1d.at[last1DIndexTimeData[1], 'h']
                prevDayLow = df_1d.at[last1DIndexTimeData[1], 'l']
                self.strategyLogger.info(f"Prev Day High: {prevDayHigh}")
                self.strategyLogger.info(f"Prev Day Low: {prevDayLow}")  



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

                    elif self.humanTime.time() >= time(15, 15):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)
                        CallTradeEntry=False
                        PutTradeEntry=False


                    elif (df.at[lastIndexTimeData[1], "c"] > prevDayHigh) & (symSide == "CE"):  
                            exitType = "Candle Stoploss Hit"
                            self.exitOrder(index, exitType)
                    elif (df.at[lastIndexTimeData[1], "c"] < prevDayLow) & (symSide == "PE"):
                            exitType = "Candle Stoploss Hit"
                            self.exitOrder(index, exitType)



            # Place orders based on conditions
            if (lastIndexTimeData[1] in df.index) & (self.humanTime.time() == time(9, 16)): 
                if prevDayLow is not None and prevDayHigh is not None:
                    if prevDayLow < (df.at[lastIndexTimeData[1],"c"]) < prevDayHigh:
                        callSym = self.getCallSym(startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry,otmFactor= 0)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])  
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 0.3 * data["c"]
                        stoploss = 1.3 * data["c"]

                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {
                                        "Target": target,
                                        "Stoploss": stoploss,
                                        "Expiry": expiryEpoch,},
                                        )

                        putSym = self.getPutSym(startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry,otmFactor= 0)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 0.3 * data["c"]
                        stoploss = 1.3 * data["c"]

                        self.entryOrder(data["c"], putSym, lotSize, "SELL", {
                                        "Target": target,
                                        "Stoploss": stoploss,
                                        "Expiry": expiryEpoch, },
                                        )
                        CallTradeEntry=True
                        PutTradeEntry=True
                        # self.strategyLogger.info(f"NextTradeEntry: {NextTradeEntry}")

                    if (df.at[lastIndexTimeData[1],"c"]) > prevDayHigh: 
                        putSym = self.getPutSym(
                            startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor= 0)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 0.3 * data["c"]
                        stoploss = 1.3 * data["c"]

                        self.entryOrder(data["c"], putSym, lotSize, "SELL", {
                                        "Target": target,
                                        "Stoploss": stoploss,
                                        "Expiry": expiryEpoch, },
                                        )
                        CallTradeEntry=True

                    if (df.at[lastIndexTimeData[1],"c"]) < prevDayLow:
                        callSym = self.getCallSym(startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry,otmFactor= 0)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])  
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 0.3 * data["c"]
                        stoploss = 1.3 * data["c"]

                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {
                                        "Target": target,
                                        "Stoploss": stoploss,
                                        "Expiry": expiryEpoch, },
                                        )
                        PutTradeEntry=True

            if PutTradeEntry:
                if (df.at[lastIndexTimeData[1],"c"]) > prevDayHigh and (self.humanTime.time() > time(9, 16)): 
                    putSym = self.getPutSym(
                        startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor= 0)
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    target = 0.3 * data["c"]
                    stoploss = 1.3 * data["c"]

                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {
                                    "Target": target,
                                    "Stoploss": stoploss,
                                    "Expiry": expiryEpoch, },
                                    )
                    PutTradeEntry=False
            
            if CallTradeEntry:
                if (df.at[lastIndexTimeData[1],"c"]) < prevDayLow and (self.humanTime.time() > time(9, 16)):
                    callSym = self.getCallSym(startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry,otmFactor= 0)
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])  
                    except Exception as e:
                        self.strategyLogger.info(e)

                    target = 0.3 * data["c"]
                    stoploss = 1.3 * data["c"]

                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {
                                    "Target": target,
                                    "Stoploss": stoploss,
                                    "Expiry": expiryEpoch, },
                                        )
                    CallTradeEntry=False



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
    startDate = datetime(2023, 1, 5, 9, 15)
    endDate = datetime(2023, 3, 31, 15, 30)

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