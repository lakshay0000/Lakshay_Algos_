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
            df_1h = getFnoBacktestData(
                indexSym, startEpoch-(86400*50), endEpoch, "1H")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_1h.dropna(inplace=True)


    # Calculate the 20-period EMA
        df_1h['EMA20'] = df_1h['c'].ewm(span=20, adjust=False).mean()

    # Calculate the 50-period EMA
        df_1h['EMA50'] = df_1h['c'].ewm(span=50, adjust=False).mean()

    # Calculate the 100-period EMA
        df_1h['EMA100'] = df_1h['c'].ewm(span=100, adjust=False).mean()

    # Calculate the 200-period EMA
        df_1h['EMA200'] = df_1h['c'].ewm(span=200, adjust=False).mean()

        df_1h.dropna(inplace=True)
        
        # Create EMA_High and EMA_Low columns
        df_1h['EMA_High'] = df_1h[['EMA20', 'EMA50', 'EMA100', 'EMA200']].max(axis=1)
        df_1h['EMA_Low'] = df_1h[['EMA20', 'EMA50', 'EMA100', 'EMA200']].min(axis=1)

        results=[]
        results = taa.stochrsi(df_1h["c"], length=14, rsi_length=14, k=3, d=3)
        df_1h["%K"] = results["STOCHRSIk_14_14_3_3"]
        df_1h["%D"] = results["STOCHRSId_14_14_3_3"]

        # Filter dataframe from timestamp greater than start time timestamp
        df_1h = df_1h[df_1h.index > startEpoch]

        # Determine crossover signals
        df_1h["%KCross80"] = np.where((df_1h["%K"] > 80) & (df_1h["%K"].shift(1) <= 80), 1, 0)
        df_1h["%KCross20"] = np.where((df_1h["%K"] < 20) & (df_1h["%K"].shift(1) >= 20), 1, 0)  
        
        df_1h["EMACross200"] = np.where((df_1h["EMA200"] > df_1h["c"]) & (df_1h["EMA200"].shift() < df_1h["c"].shift()), 1, 0)
        

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_1h.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_15Min.csv"
        )

        # Strategy Parameters
        flag1 = False
        PutEntryAllow = False
        swinghigh=None
        Closelist= []
        maxlist=[]  
        lastIndexTimeData = [0, 0]
        last1HIndexTimeData = [0, 0]
        list1=[]
        Midlist=[]
        MidFlag= False



        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        ReEntryAllow = False

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
            if (timeData-3600) in df_1h.index:
                last1HIndexTimeData.pop(0)
                last1HIndexTimeData.append(timeData-3600)

            # Strategy Specific Trading Time
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            #  # Log relevant information
            # if lastIndexTimeData[1] in df.index:
            #     self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")


            # Update current price for open positions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    if lastIndexTimeData[1] in df.index:
                        self.openPnl.at[index, "CurrentPrice"] = df.at[lastIndexTimeData[1], "c"]

            # Calculate and update PnL
            self.pnlCalculator()
            
            if ((timeData-3600) in df_1h.index):
                if df_1h.at[last1HIndexTimeData[1], "%KCross80"] == 1 and flag1== False:              
                    flag1= True
                    Closelist= []
                    self.strategyLogger.info(f"{self.humanTime}\t%K_high: {df_1h.at[last1HIndexTimeData[1], '%K']}\tclose: {df_1h.at[last1HIndexTimeData[1], 'c']}")

            if self.humanTime.date() >= (expiryDatetime - timedelta(days=1)).date():
                Currentexpiry = getExpiryData(self.timeData, baseSym)['NextExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
            
            if ((timeData-3600) in df_1h.index):
                if flag1:
                    Closelist.append(df_1h.at[last1HIndexTimeData[1], "h"])
                    if df_1h.at[last1HIndexTimeData[1], "%KCross20"] == 1:
                        flag1=False
                        swinghigh= max(Closelist)
                        maxlist.append(swinghigh)
                        self.strategyLogger.info(f"{self.humanTime}swinghigh:{swinghigh}\t%K_Low: {df_1h.at[last1HIndexTimeData[1], '%K']}\tclose: {df_1h.at[last1HIndexTimeData[1], 'c']}\tswingcomplte\t maxlist: {maxlist}")
                        if MidFlag==True:
                            MidFlag=False
                            Midlist.clear()

                        MidFlag=True


            if ((timeData-3600) in df_1h.index) and self.openPnl.empty:
                if  len(maxlist)>=2 and (df_1h.at[last1HIndexTimeData[1], "EMA_High"] - df_1h.at[last1HIndexTimeData[1], "EMA_Low"])<50:
                    last_two_max = maxlist[-2:]
                    if Midlist:
                        Midhigh = max(Midlist)
                        last_two_max.append(Midhigh)  
                             
                    # Find the maximum of the updated last_two_max list
                    Twoswinghigh = max(last_two_max)

                    PutEntryAllow = True
                    ReEntryAllow = False
                    list1.clear()
                    self.strategyLogger.info(f"{self.humanTime}\tTwoswinghigh: {Twoswinghigh}\tPutEntryAllow: {PutEntryAllow}")
            
            
            if not self.openPnl.empty and (timeData-3600) in df_1h.index:
                list1.append(df_1h.at[last1HIndexTimeData[1], "h"])
            
            if lastIndexTimeData[1] in df.index:
                UnderlyingPrice = df.at[lastIndexTimeData[1], "c"]


                                                                    

            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    # symstrike = float(row['Symbol'][-7:-2])
      

                    if UnderlyingPrice <= (row["IndexPrice"]-50):
                        exitType = "MarketStoploss"
                        self.exitOrder(index, exitType)
                        list1_high = max(list1)
                        ReEntryAllow = True
                    
                    elif (timeData-3600) in df_1h.index:
                        if df_1h.at[last1HIndexTimeData[1], "EMACross200"] == 1:
                            exitType = "EMACross200"
                            self.exitOrder(index, exitType)
                            list1_high = max(list1)
                            ReEntryAllow = True

                    # elif self.timeData >= row["Expiry"]:
                    #     exitType = "Time Up"
                    #     self.exitOrder(index, exitType)
    

            # tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            # callCounter= tradecount.get('CE',0)
            # putCounter= tradecount.get('PE',0)

            # Check for entry signals and execute orders
            if ((timeData-3600) in df_1h.index) and self.openPnl.empty:
                
                if (PutEntryAllow): 
                    if df_1h.at[last1HIndexTimeData[1], "c"]> Twoswinghigh:
                        list1.append(df_1h.at[last1HIndexTimeData[1], "h"])
                        
                        entry_price = df_1h.at[last1HIndexTimeData[1], "c"]
                        indexprice = df_1h.at[last1HIndexTimeData[1], "c"]

                        self.entryOrder(entry_price, "NIFTY50", lotSize, "BUY", {"Expiry": expiryEpoch, "IndexPrice":indexprice},)
                        PutEntryAllow = False  
                        maxlist = maxlist[-2:] 



                if (ReEntryAllow): 
                    if df_1h.at[last1HIndexTimeData[1], "c"]> list1_high:
                        self.strategyLogger.info(f"{self.humanTime}\tlist1_high: {list1_high}")

                        list1.clear()
                        list1.append(df_1h.at[last1HIndexTimeData[1], "h"])

                        entry_price = df_1h.at[last1HIndexTimeData[1], "c"]

                        indexprice = df_1h.at[last1HIndexTimeData[1], "c"]

                        self.entryOrder(entry_price, "NIFTY50", lotSize, "BUY", {"Expiry": expiryEpoch, "IndexPrice":indexprice},)
                        ReEntryAllow = False  

            if ((timeData-3600) in df_1h.index):
                if MidFlag:
                        Midlist.append(df_1h.at[last1HIndexTimeData[1], "h"])



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