import numpy as np
import talib as ta
import pandas_ta as taa
from backtestTools.expiry import getExpiryData, connectToMongo
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getFnoBacktestData

# sys.path.insert(1, '/root/backtestTools')


# Define a class algoLogic that inherits from optOverNightAlgoLogic
class algoLogic(optOverNightAlgoLogic):

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
        conn = connectToMongo()

        # Add necessary columns to the DataFrame
        col = ["Expiry"]
        self.addColumnsToOpenPnlDf(col)

        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            # Fetch historical data for backtesting
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min",conn=conn)
            df_1h = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1H",conn=conn)
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_1h.dropna(inplace=True)

        results=[]

        results = taa.supertrend(df_1h["h"], df_1h["l"], df_1h["c"], length=10, multiplier=3.0)
        # print(results)
        df_1h["Supertrend"] = results["SUPERTd_10_3.0"]
        df_1h.dropna(inplace=True)

        df_1h['Scross'] = 0
        df_1h.loc[(df_1h['Supertrend'] == 1) & (df_1h['Supertrend'].shift(1) == -1), 'Scross'] = 1
        df_1h.loc[(df_1h['Supertrend'] == -1) & (df_1h['Supertrend'].shift(1) == 1), 'Scross'] = -1
        
        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_1h.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1H.csv")


        lastIndexTimeData = [0, 0]
        last1HIndexTimeData = [0, 0]
        flag1=0
        flag2=0

        
        expiryEpoch = self.getCurrentExpiryEpoch(startEpoch, baseSym)
        expiry= datetime.fromtimestamp(expiryEpoch)

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index:
            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            if (timeData-3600) in df_1h.index:
                last1HIndexTimeData.pop(0)
                last1HIndexTimeData.append(timeData-3600)  

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
                            row["Symbol"], lastIndexTimeData[1],conn=conn)
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)
            
             # Log relevant information
            if lastIndexTimeData[1] in df.index:
                self.strategyLogger.info(
                    f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")



            # Calculate and update PnL
            self.pnlCalculator()

            if self.humanTime.date() > expiry.date() :
                expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                expiry= datetime.fromtimestamp(expiryEpoch)
                flag1=1
                flag2=0
                


            # if lastIndexTimeData[1] in df.index:
            #     nxtexpiry= getExpiryData(self.timeData +86400,baseSym)['CurrentExpiry']
            #     nxtexpdt= datetime.strptime(nxtexpiry,"%d%b%y")  
            #     nxtexpdt = nxtexpdt.replace(hour=15, minute=20)
            #     NxtexpiryEpoch = nxtexpdt.timestamp() 
                    
            
            if lastIndexTimeData[1] in df.index:
                self.strategyLogger.info(
                    f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}\texpiry:{expiryEpoch}\tflag:{flag1}\tepoch:{lastIndexTimeData[1]},\texpirydate:{expiry}")


                # Check for exit conditions and execute exit orders
                if not self.openPnl.empty:
                    for index, row in self.openPnl.iterrows():

                        if row["CurrentPrice"] <= row["Target"]:
                            exitType = "Target Hit"
                            self.exitOrder(index, exitType, row["CurrentPrice"])

                        elif row["CurrentPrice"] >= row["Stoploss"]:
                            exitType = "Stoploss Hit"
                            self.exitOrder(index, exitType, row["CurrentPrice"])

                        elif self.timeData >= row["Expiry"]:
                                exitType = "Time Up"
                                self.exitOrder(index, exitType)   


            # Check for entry signals and execute orders
            if ((timeData-3600) in df_1h.index) and (flag1==1):
                if (self.humanTime.time() == time(10, 15)) and (df_1h.at[last1HIndexTimeData[1], "Supertrend"]==-1):
                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"])
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)

                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1],conn=conn)
                    except Exception as e:
                        self.strategyLogger.info(e)

                    target = 0.3 * data["c"]
                    stoploss = 2.0 * data["c"]

                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch,"Target": target,"Stoploss": stoploss,})
                    flag1=0
                    flag2=1

                if (self.humanTime.time() == time(10, 15)) and (df_1h.at[last1HIndexTimeData[1], "Supertrend"]==1):
                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"])
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1],conn=conn)
                    except Exception as e:
                        self.strategyLogger.info(e)

                    target = 0.3 * data["c"]
                    stoploss = 2.0 * data["c"]

                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch,"Target": target,"Stoploss": stoploss,})
                    flag1=0
                    flag2=1

            
            if ((timeData-3600) in df_1h.index) and (flag2==1):
                
                if (df_1h.at[last1HIndexTimeData[1], "Scross"]==-1):

                    if (self.humanTime.date()== expiry.date()):

                        callSym = self.getCallSym(self.timeData+86400, baseSym, df.at[lastIndexTimeData[1], "c"])
                    else:
                        callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"])

                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])


                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1],conn=conn)
                    except Exception as e:
                        self.strategyLogger.info(e)

                    target = 0.3 * data["c"]
                    stoploss = 2.0 * data["c"]

                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch,"Target": target,"Stoploss": stoploss,})
                    flag2=0  

                if  (df_1h.at[last1HIndexTimeData[1], "Scross"]==1):
                    if (self.humanTime.date()== expiry.date()):

                        putSym = self.getPutSym(self.timeData+86400, baseSym, df.at[lastIndexTimeData[1], "c"])
                    else:
                        putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"])

                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1],conn=conn)
                    except Exception as e:
                        self.strategyLogger.info(e)

                    target = 0.3 * data["c"]
                    stoploss = 2.0 * data["c"]

                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch,"Target": target,"Stoploss": stoploss,})
                    flag2=0


        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]



if __name__ == "__main__":
    startTime = datetime.now()

    # Define Strategy Nomenclature
    devName = "NA"
    strategyName = "0vernight3"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2024, 3, 25, 15, 30)

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