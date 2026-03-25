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
        

        df_15min["rsi"] = ta.RSI(df_15min["c"], timeperiod=7)

        # Filter dataframe from timestamp greater than start time timestamp
        df_15min = df_15min[df_15min.index >= startEpoch]

        # Determine crossover signals
        # df_15min["%KCross80"] = np.where((df_15min["%K"] > 80) & (df_15min["%K"].shift(1) <= 80), 1, 0)
        # df_15min["%KCross20"] = np.where((df_15min["%K"] < 20) & (df_15min["%K"].shift(1) >= 20), 1, 0)
        
        # df_15min["EMACross200Below"] = np.where((df_15min["EMA200"] > df_15min["c"]) & (df_15min["EMA200"].shift() < df_15min["c"].shift()), 1, 0)
        # df_15min["EMACross200Above"] = np.where((df_15min["EMA200"] < df_15min["c"]) & (df_15min["EMA200"].shift() > df_15min["c"].shift()), 1, 0)  

        

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_15min.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_15Min.csv"
        )

        # Strategy Parameters
        flag1 = False
        flag2= False
        PutEntryAllow = True
        CallEntryAllow = True
        swinghigh=None
        swinglow=None
        Closelist= []
        maxlist=[]  
        lowlist= []
        lastIndexTimeData = [0, 0]
        last15MinIndexTimeData = [0, 0]



        Currentexpiry = getExpiryData(startEpoch, baseSym)['MonthlyExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(startEpoch, baseSym)["LotSize"])


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
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
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

            if self.humanTime.date() >= (expiryDatetime - timedelta(days=2)).date():
                Currentexpiry = getExpiryData(self.timeData+(86400*2), baseSym)['MonthlyExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()


            # HighSwing
            if ((timeData-900) in df_15min.index):
                if df_15min.at[last15MinIndexTimeData[1], "rsi"] > 70 and flag1== False:                
                    flag1= True
                    Closelist= []
                    self.strategyLogger.info(f"{self.humanTime}\t RSI: {df_15min.at[last15MinIndexTimeData[1], 'rsi']} HighSwingStarted")

            
            if ((timeData-900) in df_15min.index):
                if flag1:
                    Closelist.append(df_15min.at[last15MinIndexTimeData[1], "h"])
                    if df_15min.at[last15MinIndexTimeData[1], "rsi"] < 30:
                        flag1=False
                        swinghigh= max(Closelist)
                        self.strategyLogger.info(f"{self.humanTime}\t RSI: {df_15min.at[last15MinIndexTimeData[1], 'rsi']} swinghigh:{swinghigh} HighSwingCompleted")


            # LowSwing
            if ((timeData-900) in df_15min.index):
                if df_15min.at[last15MinIndexTimeData[1], "rsi"] < 30 and flag2== False:              
                    flag2= True
                    Closelist_low= []
                    self.strategyLogger.info(f"{self.humanTime}\t RSI: {df_15min.at[last15MinIndexTimeData[1], 'rsi']} LowSwingStarted")


                if flag2:
                    Closelist_low.append(df_15min.at[last15MinIndexTimeData[1], "l"])
                    if df_15min.at[last15MinIndexTimeData[1], "rsi"] > 70:
                        flag2=False
                        swinglow = min(Closelist_low)
                        self.strategyLogger.info(f"{self.humanTime}\t RSI: {df_15min.at[last15MinIndexTimeData[1], 'rsi']} swinglow:{swinglow} LowSwingCompleted")


            
            if lastIndexTimeData[1] in df.index:
                UnderlyingPrice = df.at[lastIndexTimeData[1], "c"]
            

            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]   

                    if self.timeData >= row["Expiry"]:
                            exitType = "Time Up"
                            self.exitOrder(index, exitType)   

                    elif symSide == "PE":
                        if row["CurrentPrice"] >= row["EntryPrice"]*1.5:
                            exitType = "ESL_Hit"
                            self.exitOrder(index, exitType)

                        elif row["CurrentPrice"] <= row["Target"]:
                            exitType = "TargetHit"
                            self.exitOrder(index, exitType)
                        
                        elif (timeData-900) in df_15min.index:
                            if UnderlyingPrice <= row["Stoploss"]:
                                exitType = "ISL"
                                self.exitOrder(index, exitType)


                    elif symSide == "CE":
                        if row["CurrentPrice"] >= row["EntryPrice"]*1.5:
                            exitType = "ESL_Hit"
                            self.exitOrder(index, exitType)

                        elif row["CurrentPrice"] <= row["Target"]:
                            exitType = "TargetHit"
                            self.exitOrder(index, exitType)

                        elif (timeData-900) in df_15min.index:
                            if UnderlyingPrice >= row["Stoploss"]:
                                exitType = "ISL"
                                self.exitOrder(index, exitType)




            tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            callCounter= tradecount.get('CE',0)
            putCounter= tradecount.get('PE',0)

            # Get the last CALL entry
            call_entries = self.openPnl[self.openPnl['Symbol'].str.endswith('CE')]
            if not call_entries.empty:
                last_call_index = call_entries.index[-1]
                last_call_row = self.openPnl.loc[last_call_index]
                last_call_symbol = last_call_row['Symbol']
                if last_call_row['CurrentPrice'] > 0.8 * last_call_row['EntryPrice']:
                    CallEntryAllow = False
                else:
                    CallEntryAllow = True

            else:
                CallEntryAllow = True
                

            # Get the last PUT entry
            put_entries = self.openPnl[self.openPnl['Symbol'].str.endswith('PE')]
            if not put_entries.empty:
                last_put_index = put_entries.index[-1]
                last_put_row = self.openPnl.loc[last_put_index]
                last_put_symbol = last_put_row['Symbol']
                if last_put_row['CurrentPrice'] > 0.8 * last_put_row['EntryPrice']:
                    PutEntryAllow = False
                else:
                    PutEntryAllow = True

            else:
                PutEntryAllow = True


            # Check for entry signals and execute orders
            if ((timeData-900) in df_15min.index):

                if putCounter<3 and PutEntryAllow: 
                    if df_15min.at[last15MinIndexTimeData[1], "rsi"] > 70 and swinglow is not None:
                        if (df_15min.at[last15MinIndexTimeData[1], "rsi"] - swinglow) < 100:
                            otmFactor = 1
                        else:
                            otmFactor = 0

                        putSym = self.getPutSym(
                            self.timeData, baseSym, swinglow, expiry= Currentexpiry, strikeDist=100, otmFactor=otmFactor)

                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        stoploss = swinglow
                        target = 0.2 * data["c"]

                        self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Stoploss":stoploss, "Target":target},)
                        swinglow = None


                if callCounter<3 and CallEntryAllow: 
                    if df_15min.at[last15MinIndexTimeData[1], "rsi"] < 30 and swinghigh is not None:
                        if (df_15min.at[last15MinIndexTimeData[1], "rsi"] - swinghigh) < 100:
                            otmFactor = 1
                        else:
                            otmFactor = 0

                        callSym = self.getCallSym(
                            self.timeData, baseSym, swinghigh, expiry= Currentexpiry, strikeDist=100, otmFactor=otmFactor)

                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        stoploss = swinghigh
                        target = 0.2 * data["c"]

                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Stoploss":stoploss, "Target":target},)
                        swinghigh = None


         # At the end of the trading day, exit all open positions   
        if not self.openPnl.empty:
            for index, row in self.openPnl.iterrows():
                exitType = "Time Up Remaining"
                self.exitOrder(index, exitType)             


            

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
    startDate = datetime(2024, 1, 1, 9, 15)
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