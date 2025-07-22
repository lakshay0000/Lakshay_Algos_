import numpy as np
import talib as ta
import pandas_ta as taa
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic 
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getEquityBacktestData, getEquityHistData, connectToMongo

# sys.path.insert(1, '/root/backtestTools')


# Define a class algoLogic that inherits from optOverNightAlgoLogic
class algoLogic(optOverNightAlgoLogic):

    # Define a method to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):
        
        conn = connectToMongo()

        # Add necessary columns to the DataFrame
        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            # Fetch historical data for backtesting
            df = getEquityBacktestData(indexSym, startEpoch, endEpoch, "1Min", conn=conn)
            df_15min = getEquityBacktestData(
                indexSym, startEpoch-(86400*50), endEpoch, "15Min", conn=conn)
            df_1d = getEquityBacktestData(indexSym, startEpoch-(86400*50), endEpoch, "1D")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}") 
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_15min.dropna(inplace=True)
        df_1d.dropna(inplace=True)

        # Add 33360 to the index to match the timestamp
        df_1d.index = df_1d.index + 33360
        df_1d.ti = df_1d.ti + 33360


    # Calculate ATR (default period = 14)
        df_1d['ATR'] = taa.atr(high=df_1d['h'], low=df_1d['l'], close=df_1d['c'], length=14)

        df_1d.dropna(inplace=True)


    # Calculate the 20-period EMA
        df_15min['EMA20'] = df_15min['c'].ewm(span=20, adjust=False).mean()

    # Calculate the 50-period EMA
        df_15min['EMA50'] = df_15min['c'].ewm(span=50, adjust=False).mean()

    # Calculate the 100-period EMA
        df_15min['EMA100'] = df_15min['c'].ewm(span=100, adjust=False).mean()

    # Calculate the 200-period EMA
        df_15min['EMA200'] = df_15min['c'].ewm(span=200, adjust=False).mean()

        
        # Create EMA_High and EMA_Low columns
        df_15min['EMA_High'] = df_15min[['EMA20', 'EMA50', 'EMA100', 'EMA200']].max(axis=1)
        df_15min['EMA_Low'] = df_15min[['EMA20', 'EMA50', 'EMA100', 'EMA200']].min(axis=1)

        results=[]
        results = taa.stochrsi(df_15min["c"], length=14, rsi_length=14, k=3, d=3)
        df_15min["%K"] = results["STOCHRSIk_14_14_3_3"]
        df_15min["%D"] = results["STOCHRSId_14_14_3_3"]

        df_15min.dropna(inplace=True)

        # Filter dataframe from timestamp greater than start time timestamp
        df_15min = df_15min[df_15min.index >= startEpoch]
        df_1d = df_1d[df_1d.index >= startEpoch-86340]

        # Determine crossover signals
        df_15min["%KCross80"] = np.where((df_15min["%K"] > 80) & (df_15min["%K"].shift(1) <= 80), 1, 0)
        df_15min["%KCross20"] = np.where((df_15min["%K"] < 20) & (df_15min["%K"].shift(1) >= 20), 1, 0)
        
        df_15min["EMACross200Below"] = np.where((df_15min["EMA200"] > df_15min["c"]) & (df_15min["EMA200"].shift() < df_15min["c"].shift()), 1, 0)
        df_15min["EMACross200Above"] = np.where((df_15min["EMA200"] < df_15min["c"]) & (df_15min["EMA200"].shift() > df_15min["c"].shift()), 1, 0)

        

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_15min.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_15Min.csv"
        )
        df_1d.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1d.csv"
        )


        # Strategy Parameters
        flag1 = False
        flag2= False
        PutEntryAllow = False
        CallEntryAllow = False
        swinghigh=None
        Closelist= []
        maxlist=[]  
        lowlist= []
        lastIndexTimeData = [0, 0]
        last15MinIndexTimeData = [0, 0]
        list1_high=[]
        list1_low=[]
        Midlist=[]
        MidFlag= False
        Midlist_low=[]
        MidFlag_low= False
        prev_ATR = None


        Currentexpiry = getExpiryData(startEpoch, baseSym, conn=conn)['MonthlyExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        StrikeDist = int(getExpiryData(startEpoch, baseSym, conn=conn)["StrikeDist"])
        lotSize = int(getExpiryData(startEpoch, baseSym)["LotSize"])
        PutReEntryAllow = False
        CallReEntryAllow = False

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

            #Updating daily index
            prev_day = timeData - 86400
            if timeData in df_1d.index:
                #check if previoud day exists in 1d data
                while prev_day not in df_1d.index:
                    prev_day = prev_day - 86400

            if prev_day in df_1d.index:
                prev_ATR = (df_1d.at[prev_day, 'ATR'])/4



            # Update current price for open positions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = getEquityHistData(
                            row["Symbol"], lastIndexTimeData[1], conn=conn)
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info("NO DATA FOUND FOR " + row["Symbol"])

            # Calculate and update PnL
            self.pnlCalculator()
            
            if ((timeData-900) in df_15min.index):
                if df_15min.at[last15MinIndexTimeData[1], "%KCross80"] == 1 and flag1== False:              
                    flag1= True
                    Closelist= []
                    self.strategyLogger.info(f"{self.humanTime}\t%K_high: {df_15min.at[last15MinIndexTimeData[1], '%K']}\tclose: {df_15min.at[last15MinIndexTimeData[1], 'c']}")

            if self.humanTime.date() >= (expiryDatetime - timedelta(days=1)).date():
                Currentexpiry = getExpiryData(self.timeData+(86400*2), baseSym, conn=conn)['MonthlyExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
                StrikeDist = int(getExpiryData(startEpoch, baseSym, conn=conn)["StrikeDist"])            

            
            if ((timeData-900) in df_15min.index):
                if flag1:
                    Closelist.append(df_15min.at[last15MinIndexTimeData[1], "h"])
                    if df_15min.at[last15MinIndexTimeData[1], "%KCross20"] == 1:
                        flag1=False
                        swinghigh= max(Closelist)
                        maxlist.append(swinghigh)
                        self.strategyLogger.info(f"{self.humanTime}swinghigh:{swinghigh}\t%K_Low: {df_15min.at[last15MinIndexTimeData[1], '%K']}\tclose: {df_15min.at[last15MinIndexTimeData[1], 'c']}\tHighswingcomplte")
                        if MidFlag==True:
                            MidFlag=False
                            Midlist.clear()

                        MidFlag=True

            if ((timeData-900) in df_15min.index):
                if df_15min.at[last15MinIndexTimeData[1], "%KCross20"] == 1 and flag2== False:              
                    flag2= True
                    Closelist_low= []
                    self.strategyLogger.info(f"{self.humanTime}\t%K_Low: {df_15min.at[last15MinIndexTimeData[1], '%K']}\tclose: {df_15min.at[last15MinIndexTimeData[1], 'c']}")



                if flag2:
                    Closelist_low.append(df_15min.at[last15MinIndexTimeData[1], "l"])
                    if df_15min.at[last15MinIndexTimeData[1], "%KCross80"] == 1:
                        flag2=False
                        swinglow = min(Closelist_low)
                        lowlist.append(swinglow)
                        self.strategyLogger.info(f"{self.humanTime}\tswinglow:{swinglow}\t%K_Low: {df_15min.at[last15MinIndexTimeData[1], '%K']}\tclose: {df_15min.at[last15MinIndexTimeData[1], 'c']}\tLowswingcomplte")
                        if MidFlag_low==True:
                            MidFlag_low=False
                            Midlist_low.clear()

                        MidFlag_low=True

            tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            callCounter= tradecount.get('CE',0)
            putCounter= tradecount.get('PE',0)


            if ((timeData-900) in df_15min.index):
                if len(maxlist)>=2 and len(lowlist)>=2 and (df_15min.at[last15MinIndexTimeData[1], "EMA_High"] - df_15min.at[last15MinIndexTimeData[1], "EMA_Low"])<prev_ATR:
                    if len(maxlist)>=2:
                        last_two_max = maxlist[-2:]
                        if Midlist:
                            Midhigh = max(Midlist)
                            last_two_max.append(Midhigh)  
                                
                        # Find the maximum of the updated last_two_max list
                        Twoswinghigh = max(last_two_max)

                    if len(lowlist)>=2:
                        last_two_min = lowlist[-2:]
                        if Midlist_low:
                            Midlow = min(Midlist_low)
                            last_two_min.append(Midlow)  
                                
                        # Find the maximum of the updated last_two_max list
                        Twoswinglow = min(last_two_min)

                    PutEntryAllow = True
                    CallEntryAllow = True
                    PutReEntryAllow = False
                    CallReEntryAllow = False
                    atr_SL = prev_ATR
                    if putCounter==0:
                        list1_high.clear()
                    if callCounter==0:
                        list1_low.clear()
                    self.strategyLogger.info(f"{self.humanTime}\tTwoswinghigh: {Twoswinghigh}\tTwoswinglow: {Twoswinglow}\tPUT&CALLEntryAllow: TRUE\t prev_ATR: {prev_ATR}")

            
            if not self.openPnl.empty and (timeData-900) in df_15min.index:
                if putCounter>0:
                    list1_high.append(df_15min.at[last15MinIndexTimeData[1], "h"])
                if callCounter>0:
                    list1_low.append(df_15min.at[last15MinIndexTimeData[1], "l"])
            
            if lastIndexTimeData[1] in df.index:
                UnderlyingPrice = df.at[lastIndexTimeData[1], "c"]


                                                                    

            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]      

                    if symSide == "PE":
                        if UnderlyingPrice <= (row["IndexPrice"]-atr_SL):
                            exitType = "MarketStoploss"
                            self.exitOrder(index, exitType)
                            list1_high_V = max(list1_high)
                            PutReEntryAllow = True
                            PutEntryAllow = False
                        
                        elif (timeData-900) in df_15min.index:
                            if df_15min.at[last15MinIndexTimeData[1], "EMACross200Below"] == 1:
                                exitType = "EMACross200"
                                self.exitOrder(index, exitType)
                                list1_high_V = max(list1_high)
                                PutReEntryAllow = True
                                PutEntryAllow = False

                        elif row["CurrentPrice"] <= row["Target"]:
                            exitType = "TargetHit"
                            self.exitOrder(index, exitType)
                            putSym = self.getPutSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry,strikeDist= StrikeDist, conn=conn)

                            try:
                                data = getEquityHistData(
                                    putSym, lastIndexTimeData[1], conn=conn)
                            except Exception as e:
                                self.strategyLogger.info(e)

                            target = 0.3 * data["c"]

                            self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "IndexPrice":indexprice, "Target":target},)

                        elif self.timeData >= row["Expiry"]:
                            exitType = "Time Up"
                            self.exitOrder(index, exitType)
                            putSym = self.getPutSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry,strikeDist= StrikeDist, conn=conn)

                            try:
                                data = getEquityHistData(
                                    putSym, lastIndexTimeData[1], conn=conn)
                            except Exception as e:
                                self.strategyLogger.info(e)

                            target = 0.3 * data["c"]

                            self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "IndexPrice":indexprice, "Target":target},)

                    elif symSide == "CE":
                        if UnderlyingPrice >= (row["IndexPrice"]+atr_SL):
                            exitType = "MarketStoploss"
                            self.exitOrder(index, exitType)
                            list1_low_V = min(list1_low)
                            CallReEntryAllow = True
                            CallEntryAllow = False
                        
                        elif (timeData-900) in df_15min.index:
                            if df_15min.at[last15MinIndexTimeData[1], "EMACross200Above"] == 1:
                                exitType = "EMACross200"
                                self.exitOrder(index, exitType)
                                list1_low_V = min(list1_low)
                                CallReEntryAllow = True
                                CallEntryAllow = False

                        elif row["CurrentPrice"] <= row["Target"]:
                            exitType = "TargetHit"
                            self.exitOrder(index, exitType)
                            callSym = self.getCallSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry,strikeDist= StrikeDist, conn=conn)

                            try:
                                data = getEquityHistData(
                                    callSym, lastIndexTimeData[1], conn=conn)
                            except Exception as e:
                                self.strategyLogger.info(e)

                            target = 0.3 * data["c"]

                            self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "IndexPrice":indexprice, "Target":target},)

                        elif self.timeData >= row["Expiry"]:
                            exitType = "Time Up"
                            self.exitOrder(index, exitType)
                            callSym = self.getCallSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry,strikeDist= StrikeDist, conn=conn)

                            try:
                                data = getEquityHistData(
                                    callSym, lastIndexTimeData[1], conn=conn)
                            except Exception as e:
                                self.strategyLogger.info(e)

                            target = 0.3 * data["c"]

                            self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "IndexPrice":indexprice, "Target":target},)



            # tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            # callCounter= tradecount.get('CE',0)
            # putCounter= tradecount.get('PE',0)

            # Check for entry signals and execute orders
            if ((timeData-900) in df_15min.index) and self.openPnl.empty:
                
                if (PutEntryAllow): 
                    if df_15min.at[last15MinIndexTimeData[1], "c"]> Twoswinghigh:
                        list1_high.append(df_15min.at[last15MinIndexTimeData[1], "h"])
                        self.strategyLogger.info(f"{self.humanTime}\t{self.timeData}\t{baseSym}\tclose:{df_15min.at[last15MinIndexTimeData[1], 'c']}\texpiry: {Currentexpiry}")
                        putSym = self.getPutSym(
                            self.timeData, baseSym, df_15min.at[last15MinIndexTimeData[1], "c"],expiry= Currentexpiry,strikeDist= StrikeDist, conn=conn)
                        self.strategyLogger.info(f"{self.humanTime}\tputSym: {putSym}")

                        try:
                            data = getEquityHistData(
                                putSym, lastIndexTimeData[1], conn=conn)
                        except Exception as e:
                            self.strategyLogger.info(e)

                        indexprice = df_15min.at[last15MinIndexTimeData[1], "c"]
                        target = 0.3 * data["c"]

                        self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "IndexPrice":indexprice, "Target":target},)
                        PutEntryAllow = False  
                        maxlist = maxlist[-2:]
                
                if (CallEntryAllow):
                    if df_15min.at[last15MinIndexTimeData[1], "c"]< Twoswinglow:
                        list1_low.append(df_15min.at[last15MinIndexTimeData[1], "l"])
                        self.strategyLogger.info(f"{self.humanTime}\t{self.timeData}\t{baseSym}\tclose:{df_15min.at[last15MinIndexTimeData[1], 'c']}\texpiry: {Currentexpiry}")
                        callSym = self.getCallSym(
                            self.timeData, baseSym, df_15min.at[last15MinIndexTimeData[1], "c"],expiry= Currentexpiry,strikeDist= StrikeDist, conn=conn)
                        self.strategyLogger.info(f"{self.humanTime}\tcallSym: {callSym}")

                        # data_df = getFnoBacktestData(callSym, startEpoch, endEpoch, "1Min")
                        # data_df.to_csv("CallSym.csv")

                        try:
                            data = getEquityHistData(
                                callSym, lastIndexTimeData[1], conn=conn)
                        except Exception as e:
                            self.strategyLogger.info(e)

                        indexprice = df_15min.at[last15MinIndexTimeData[1], "c"]
                        target = 0.3 * data["c"]

                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "IndexPrice":indexprice, "Target":target},)
                        CallEntryAllow = False  
                        lowlist = lowlist[-2:]



                if (PutReEntryAllow): 
                    if df_15min.at[last15MinIndexTimeData[1], "c"]> list1_high_V:
                        self.strategyLogger.info(f"{self.humanTime}\tlist1_high: {list1_high_V}\tlist1: {list1_high}")

                        list1_high.clear()
                        list1_high.append(df_15min.at[last15MinIndexTimeData[1], "h"])
                        putSym = self.getPutSym(
                            self.timeData, baseSym, df_15min.at[last15MinIndexTimeData[1], "c"],expiry= Currentexpiry,strikeDist= StrikeDist, conn=conn)

                        try:
                            data = getEquityHistData(
                                putSym, lastIndexTimeData[1], conn=conn)
                        except Exception as e:
                            self.strategyLogger.info(e)

                        indexprice = df_15min.at[last15MinIndexTimeData[1], "c"]
                        target = 0.3 * data["c"]

                        self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "IndexPrice":indexprice, "Target":target},)
                        PutReEntryAllow = False  

                if (CallReEntryAllow): 
                    if df_15min.at[last15MinIndexTimeData[1], "c"]< list1_low_V:
                        self.strategyLogger.info(f"{self.humanTime}\tlist1_low: {list1_low_V}")

                        list1_low.clear()
                        list1_low.append(df_15min.at[last15MinIndexTimeData[1], "l"])
                        callSym = self.getCallSym(
                            self.timeData, baseSym, df_15min.at[last15MinIndexTimeData[1], "c"],expiry= Currentexpiry,strikeDist= StrikeDist, conn=conn)

                        try:
                            data = getEquityHistData(
                                callSym, lastIndexTimeData[1], conn=conn)
                        except Exception as e:
                            self.strategyLogger.info(e)

                        indexprice = df_15min.at[last15MinIndexTimeData[1], "c"]
                        target = 0.3 * data["c"]

                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "IndexPrice":indexprice, "Target":target},)
                        CallReEntryAllow = False  

            
            if ((timeData-900) in df_15min.index):
                if MidFlag:
                        Midlist.append(df_15min.at[last15MinIndexTimeData[1], "h"])

            if ((timeData-900) in df_15min.index):
                if MidFlag_low:
                        Midlist_low.append(df_15min.at[last15MinIndexTimeData[1], "l"])


            


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
    startDate = datetime(2024, 1, 2, 9, 15)
    endDate = datetime(2024, 12, 31, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "AUBANK"
    indexName = "AUBANK"

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