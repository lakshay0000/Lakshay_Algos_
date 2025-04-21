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
        flag1=0


        for timeData in df.index:

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)

            self.timeData = timeData
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

           #skip times period other than trading hours than ()
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue
            # Strategy Specific Trading Time
            if (self.humanTime.time() < time(9, 20)) | (self.humanTime.time() > time(15, 25)):
                continue

            # Update current price for open positions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)
            
             # Log relevant information
            if lastIndexTimeData[1] in df.index:
                self.strategyLogger.info(
                    f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")



            # Calculate and update PnL
            self.pnlCalculator()

            if lastIndexTimeData[1] in df.index:
                expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym) 

            if lastIndexTimeData[1] in df.index:
                if (self.timeData >= expiryEpoch) and (flag1==0):
                    flag1=1
                    
            
            if lastIndexTimeData[1] in df.index:
                self.strategyLogger.info(
                    f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}\texpiry:{expiryEpoch}\tflag:{flag1}\tepoch:{lastIndexTimeData[1]}")


            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    if self.timeData >= row["Expiry"]:
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)   


            # Check for entry signals and execute orders
            
            if (lastIndexTimeData[1] in df.index) and (self.humanTime.time() < time(14, 15)):
                
                if (self.humanTime.time() >= time(10, 0)) and (flag1==1):
                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=2)
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch,})


                if (self.humanTime.time() >= time(10, 0)) and (flag1==1):
                    callSym = self.getCallSym(
                        self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=4)
                    expiryEpoch = self.getCurrentExpiryEpoch(
                        self.timeData, baseSym)
                    lotSize = int(getExpiryData(
                        self.timeData, baseSym)["LotSize"])

                    try:
                        data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch,})     

                if (self.humanTime.time() >= time(10, 0)) and (flag1==1):
                    callSym = self.getCallSym(
                        self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=4)
                    expiryEpoch = self.getCurrentExpiryEpoch(
                        self.timeData, baseSym)
                    lotSize = int(getExpiryData(
                        self.timeData, baseSym)["LotSize"])

                    try:
                        data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch,})  
                    
                 


                if (self.humanTime.time() >= time(10, 0)) and (flag1==1):
                    callSym = self.getCallSym(
                        self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=6)
                    expiryEpoch = self.getCurrentExpiryEpoch(
                        self.timeData, baseSym)
                    lotSize = int(getExpiryData(
                        self.timeData, baseSym)["LotSize"])

                    try:
                        data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch,})  
                          

                if (self.humanTime.time() >= time(10, 0)) and (flag1==1):
                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=2)
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch, },)


                if (self.humanTime.time() >= time(10, 0)) and (flag1==1):
                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=4)
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, },)

                if (self.humanTime.time() >= time(10, 0)) and (flag1==1):
                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=4)
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, },)

                if (self.humanTime.time() >= time(10, 0)) and (flag1==1):
                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=6)
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch, },)
                    flag1= 0

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]



if __name__ == "__main__":
    startTime = datetime.now()

    # Define Strategy Nomenclature
    devName = "NA"
    strategyName = "overnight3"
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

    # print("Calculating Daily Pnl")
    # dr = calculateDailyReport(
    #     closedPnl, fileDir, timeFrame=timedelta(minutes=5), mtm=True
    # )

    # limitCapital(closedPnl, fileDir, maxCapitalAmount=1000)

    # generateReportFile(dr, fileDir)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")