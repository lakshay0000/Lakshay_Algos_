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
            df_15min = getFnoBacktestData(
                indexSym, startEpoch-(86400*50), endEpoch, "15Min")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_15min.dropna(inplace=True)


    # Calculate the 20-period EMA
        df_15min['EMA20'] = df_15min['c'].ewm(span=20, adjust=False).mean()

    # Calculate the 50-period EMA
        df_15min['EMA50'] = df_15min['c'].ewm(span=50, adjust=False).mean()

    # Calculate the 100-period EMA
        df_15min['EMA100'] = df_15min['c'].ewm(span=100, adjust=False).mean()

    # Calculate the 200-period EMA
        df_15min['EMA200'] = df_15min['c'].ewm(span=200, adjust=False).mean()

        df_15min.dropna(inplace=True)
        
        # Create EMA_High and EMA_Low columns
        df_15min['EMA_High'] = df_15min[['EMA20', 'EMA50', 'EMA100', 'EMA200']].max(axis=1)
        df_15min['EMA_Low'] = df_15min[['EMA20', 'EMA50', 'EMA100', 'EMA200']].min(axis=1)

        results=[]
        results = taa.stochrsi(df_15min["c"], length=14, rsi_length=14, k=3, d=3)
        df_15min["%K"] = results["STOCHRSIk_14_14_3_3"]
        df_15min["%D"] = results["STOCHRSId_14_14_3_3"]

        # Filter dataframe from timestamp greater than start time timestamp
        df_15min = df_15min[df_15min.index > startEpoch]

        # Determine crossover signals
        df_15min["%KCross80"] = np.where((df_15min["%K"] > 80) & (df_15min["%K"].shift(1) <= 80), 1, 0)
        df_15min["%KCross20"] = np.where((df_15min["%K"] < 20) & (df_15min["%K"].shift(1) >= 20), 1, 0)
        
        df_15min["EMACross200"] = np.where((df_15min["EMA200"] > df_15min["c"]) & (df_15min["EMA200"].shift() < df_15min["c"].shift()), 1, 0)
        

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_15min.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_15Min.csv"
        )

        # Strategy Parameters
        flag1 = False
        flag2 = False
        Closelist= []
        lastIndexTimeData = [0, 0]
        last15MinIndexTimeData = [0, 0]




        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        swinghigh = None
        swinglow = None
        

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
            if (timeData-900) in df_15min.index:
                last15MinIndexTimeData.pop(0)
                last15MinIndexTimeData.append(timeData-900)

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
            
            if ((timeData-900) in df_15min.index):
                if df_15min.at[last15MinIndexTimeData[1], "%KCross80"] == 1 and flag1== False:              
                    flag1= True
                    Closelist= []
                    self.strategyLogger.info(f"{self.humanTime}\t%K_high: {df_15min.at[last15MinIndexTimeData[1], '%K']}\tclose: {df_15min.at[last15MinIndexTimeData[1], 'c']}")

            if self.humanTime.date() >= (expiryDatetime - timedelta(days=1)).date():
                Currentexpiry = getExpiryData(self.timeData, baseSym)['NextExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
            
            if ((timeData-900) in df_15min.index):
                if flag1:
                    Closelist.append(df_15min.at[last15MinIndexTimeData[1], "h"])
                    if df_15min.at[last15MinIndexTimeData[1], "%KCross20"] == 1: 
                        flag1=False
                        swinghigh= max(Closelist)
                        self.strategyLogger.info(f"{self.humanTime}swinghigh:{swinghigh}\t%K_Low: {df_15min.at[last15MinIndexTimeData[1], '%K']}\tclose: {df_15min.at[last15MinIndexTimeData[1], 'c']}\tHighswingcomplte")


            if ((timeData-900) in df_15min.index):
                if df_15min.at[last15MinIndexTimeData[1], "%KCross20"] == 1 and flag2== False:              
                    flag2= True
                    Low_Closelist= []
                    self.strategyLogger.info(f"{self.humanTime}\t%K_Low: {df_15min.at[last15MinIndexTimeData[1], '%K']}\tclose: {df_15min.at[last15MinIndexTimeData[1], 'c']}")

            if flag2:
                Low_Closelist.append(df_15min.at[last15MinIndexTimeData[1], "l"])
                if df_15min.at[last15MinIndexTimeData[1], "%KCross80"] == 1:
                    flag2=False
                    swinglow = min(Low_Closelist)
                    self.strategyLogger.info(f"{self.humanTime}\tswinglow:{swinglow}\t%K_Low: {df_15min.at[last15MinIndexTimeData[1], '%K']}\tclose: {df_15min.at[last15MinIndexTimeData[1], 'c']}\tLowswingcomplte")  
        


            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]
                    

                    if row["CurrentPrice"] <= row["Target"]:
                        exitType = "TargetHit"
                        self.exitOrder(index, exitType)
                        if symSide == "PE":
                            putSym = self.getPutSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)

                            try:
                                data = self.fetchAndCacheFnoHistData(
                                    putSym, lastIndexTimeData[1])
                            except Exception as e:
                                self.strategyLogger.info(e)

                            target = 0.3 * data["c"]
                            Stoploss = 1.3 * data["c"]


                            self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Stoploss": Stoploss, "Target":target},)

                        elif symSide == "CE":
                            callSym = self.getCallSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)

                            try:
                                data = self.fetchAndCacheFnoHistData(
                                    callSym, lastIndexTimeData[1])
                            except Exception as e:
                                self.strategyLogger.info(e)

                            target = 0.3 * data["c"]
                            Stoploss = 1.3 * data["c"]


                            self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Stoploss": Stoploss, "Target":target},)

                    elif row["CurrentPrice"] >= row["Stoploss"]:
                        exitType = "30%Stoploss"
                        self.exitOrder(index, exitType)


                    elif self.timeData >= row["Expiry"]:
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)
                        if symSide == "PE":
                            putSym = self.getPutSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)

                            try:
                                data = self.fetchAndCacheFnoHistData(
                                    putSym, lastIndexTimeData[1])
                            except Exception as e:
                                self.strategyLogger.info(e)

                            target = 0.3 * data["c"]
                            Stoploss = 1.3 * data["c"]

                            self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Stoploss": Stoploss, "Target":target},)

                        elif symSide == "CE":
                            callSym = self.getCallSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)

                            try:
                                data = self.fetchAndCacheFnoHistData(
                                    callSym, lastIndexTimeData[1])
                            except Exception as e:
                                self.strategyLogger.info(e)

                            target = 0.3 * data["c"]
                            Stoploss = 1.3 * data["c"]

                            self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Stoploss": Stoploss, "Target":target},)


                
    

            # tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            # callCounter= tradecount.get('CE',0)
            # putCounter= tradecount.get('PE',0)

            # Check for entry signals and execute orders
            if ((timeData-900) in df_15min.index) and self.openPnl.empty:
                

                if swinghigh is not None and df_15min.at[last15MinIndexTimeData[1], "c"]> swinghigh:
                    putSym = self.getPutSym(
                        self.timeData, baseSym, df_15min.at[last15MinIndexTimeData[1], "c"],expiry= Currentexpiry)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    target = 0.3 * data["c"]
                    Stoploss = 1.3 * data["c"]


                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Stoploss": Stoploss, "Target":target},)
                    self.strategyLogger.info(f"{self.humanTime}\t HighBreakoutTrade: {swinghigh}")


                if swinglow is not None and df_15min.at[last15MinIndexTimeData[1], "c"]< swinglow:
                    callSym = self.getCallSym(
                        self.timeData, baseSym, df_15min.at[last15MinIndexTimeData[1], "c"],expiry= Currentexpiry)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    target = 0.3 * data["c"]
                    Stoploss = 1.3 * data["c"]


                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Stoploss": Stoploss, "Target":target},)
                    self.strategyLogger.info(f"{self.humanTime}\t LowBreakoutTrade: {swinglow}")  



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
    startDate = datetime(2021, 1, 1, 9, 15)
    endDate = datetime(2025, 3, 31, 15, 30)

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