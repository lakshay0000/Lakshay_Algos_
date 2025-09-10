import threading
import talib
import pandas_ta as taa
import logging
import numpy as np
import multiprocessing
from termcolor import colored, cprint
from datetime import datetime, time
from backtestTools.util import setup_logger
from backtestTools.histData import getEquityHistData,connectToMongo
from backtestTools.histData import getEquityBacktestData
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from datetime import datetime, timedelta
from backtestTools.util import createPortfolio, calculateDailyReport, limitCapital, generateReportFile


class algoLogic(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "EquityOvernight":
            raise Exception("Strategy Name Mismatch")

        # Calculate total number of backtests
        total_backtests = sum(len(batch) for batch in portfolio)
        completed_backtests = 0
        cprint(
            f"Backtesting: {self.strategyName} UID: {self.fileDirUid}", "green")
        print(colored("Backtesting 0% complete.", "light_yellow"), end="\r")

        for batch in portfolio:
            processes = []
            for stock in batch:
                p = multiprocessing.Process(
                    target=self.backtest, args=(stock, startDate, endDate))
                p.start()
                processes.append(p)

            # Wait for all processes to finish
            for p in processes:
                p.join()
                completed_backtests += 1
                percent_done = (completed_backtests / total_backtests) * 100
                print(colored(f"Backtesting {percent_done:.2f}% complete.", "light_yellow"), end=(
                    "\r" if percent_done != 100 else "\n"))

        return self.fileDir["backtestResultsStrategyUid"], self.combinePnlCsv()

    def backtest(self, stockName, startDate, endDate):
        conn = connectToMongo()

        # Set start and end timestamps for data retrieval
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()

        stockAlgoLogic = equityOverNightAlgoLogic(stockName, self.fileDir)

        logger = setup_logger("strategyLogger",f"{self.fileDir['backtestResultsStrategyLogs']}{stockName}_log.log",)
        logger.propagate = False

        try:
            # Subtracting 2592000 to subtract 90 days from startTimeEpoch
            df = getEquityBacktestData(
                stockName, startTimeEpoch, endTimeEpoch, "1Min",conn=conn)
            df_15min = getEquityBacktestData(
                stockName, startTimeEpoch-(86400*50), endTimeEpoch, "15Min",conn=conn)
            df_1d = getEquityBacktestData(stockName, startTimeEpoch-(86400*50), endTimeEpoch, "1D")
        except Exception as e:
        # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {stockName} in range {startDate} to {endDate}")
            raise Exception(e)   
        
        try:
            df_15min.dropna(inplace=True)
            df.dropna(inplace=True)
            df_1d.dropna(inplace=True)
        except:
            self.strategyLogger.info(f"Data not found for {stockName}")
            return
        
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

        df_15min.dropna(inplace=True)
        
        # Create EMA_High and EMA_Low columns
        df_15min['EMA_High'] = df_15min[['EMA20', 'EMA50', 'EMA100', 'EMA200']].max(axis=1)
        df_15min['EMA_Low'] = df_15min[['EMA20', 'EMA50', 'EMA100', 'EMA200']].min(axis=1)

        results=[]
        results = taa.stochrsi(df_15min["c"], length=14, rsi_length=14, k=3, d=3)
        df_15min["%K"] = results["STOCHRSIk_14_14_3_3"]
        df_15min["%D"] = results["STOCHRSId_14_14_3_3"]

        # Filter dataframe from timestamp greater than start time timestamp
        df_15min = df_15min[df_15min.index >= startTimeEpoch]
        df_1d = df_1d[df_1d.index >= startTimeEpoch-86340]

        # Determine crossover signals
        df_15min["%KCross80"] = np.where((df_15min["%K"] > 80) & (df_15min["%K"].shift(1) <= 80), 1, 0)
        df_15min["%KCross20"] = np.where((df_15min["%K"] < 20) & (df_15min["%K"].shift(1) >= 20), 1, 0) 
        
        df_15min["EMACross200Below"] = np.where((df_15min["EMA200"] > df_15min["c"]) & (df_15min["EMA200"].shift() < df_15min["c"].shift()), 1, 0)
        df_15min["EMACross200Above"] = np.where((df_15min["EMA200"] < df_15min["c"]) & (df_15min["EMA200"].shift() > df_15min["c"].shift()), 1, 0)

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{stockName}_1Min.csv")
        df_15min.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{stockName}_15Min.csv")
        df_1d.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{stockName}_1D.csv")

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
        amountPerTrade = 100000
        PutReEntryAllow = False
        CallReEntryAllow = False


        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            stockAlgoLogic.timeData = float(timeData)
            stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)
            print(stockAlgoLogic.humanTime)


            # Skip time periods outside trading hours
            if (stockAlgoLogic.humanTime.time() < time(9, 16)) | (stockAlgoLogic.humanTime.time() > time(15, 30)):
                continue

            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            if (timeData-900) in df_15min.index:
                last15MinIndexTimeData.pop(0)
                last15MinIndexTimeData.append(timeData-900)

            # Strategy Specific Trading Time
            if (stockAlgoLogic.humanTime.time() < time(9, 16)) | (stockAlgoLogic.humanTime.time() > time(15, 25)):
                continue

            #  # Log relevant information
            # if lastIndexTimeData[1] in df.index:
            #     stockAlgoLogic.strategyLogger.info(f"Datetime: {stockAlgoLogic.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")

            #Updating daily index
            prev_day = timeData - 86400
            if timeData in df_1d.index:
                #check if previoud day exists in 1d data
                while prev_day not in df_1d.index:
                    prev_day = prev_day - 86400

            if prev_day in df_1d.index:
                prev_ATR = (df_1d.at[prev_day, 'ATR'])/4


            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    try:
                        data = getEquityHistData(
                            row["Symbol"], lastIndexTimeData[1],conn=conn)
                        stockAlgoLogic.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        logger.info(f"{stockAlgoLogic.humanTime} NO DATA FOUND FOR " + row["Symbol"])



            stockAlgoLogic.pnlCalculator()

            if ((timeData-900) in df_15min.index):
                if df_15min.at[last15MinIndexTimeData[1], "%KCross80"] == 1 and flag1== False:              
                    flag1= True
                    Closelist= []
                    logger.info(f"{stockAlgoLogic.humanTime}\t%K_high: {df_15min.at[last15MinIndexTimeData[1], '%K']}\tclose: {df_15min.at[last15MinIndexTimeData[1], 'c']}")          

            
            if ((timeData-900) in df_15min.index):
                if flag1:
                    Closelist.append(df_15min.at[last15MinIndexTimeData[1], "h"])
                    if df_15min.at[last15MinIndexTimeData[1], "%KCross20"] == 1:
                        flag1=False
                        swinghigh= max(Closelist)
                        maxlist.append(swinghigh)
                        logger.info(f"{stockAlgoLogic.humanTime}swinghigh:{swinghigh}\t%K_Low: {df_15min.at[last15MinIndexTimeData[1], '%K']}\tclose: {df_15min.at[last15MinIndexTimeData[1], 'c']}\tHighswingcomplte")
                        if MidFlag==True:
                            MidFlag=False
                            Midlist.clear()

                        MidFlag=True

            if ((timeData-900) in df_15min.index):
                if df_15min.at[last15MinIndexTimeData[1], "%KCross20"] == 1 and flag2== False:              
                    flag2= True
                    Closelist_low= []
                    logger.info(f"{stockAlgoLogic.humanTime}\t%K_Low: {df_15min.at[last15MinIndexTimeData[1], '%K']}\tclose: {df_15min.at[last15MinIndexTimeData[1], 'c']}")



                if flag2:
                    Closelist_low.append(df_15min.at[last15MinIndexTimeData[1], "l"])
                    if df_15min.at[last15MinIndexTimeData[1], "%KCross80"] == 1:
                        flag2=False
                        swinglow = min(Closelist_low)
                        lowlist.append(swinglow)
                        logger.info(f"{stockAlgoLogic.humanTime}\tswinglow:{swinglow}\t%K_Low: {df_15min.at[last15MinIndexTimeData[1], '%K']}\tclose: {df_15min.at[last15MinIndexTimeData[1], 'c']}\tLowswingcomplte")
                        if MidFlag_low==True:
                            MidFlag_low=False
                            Midlist_low.clear()

                        MidFlag_low=True

            tradecount = stockAlgoLogic.openPnl['PositionStatus'].value_counts()
            callCounter= tradecount.get(1,0)
            putCounter= tradecount.get(-1,0)

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
                    logger.info(f"{stockAlgoLogic.humanTime}\tTwoswinghigh: {Twoswinghigh}\tTwoswinglow: {Twoswinglow}\tPUT&CALLEntryAllow: TRUE\t prev_ATR: {prev_ATR}")

            
            if not stockAlgoLogic.openPnl.empty and (timeData-900) in df_15min.index:
                if putCounter>0:
                    list1_low.append(df_15min.at[last15MinIndexTimeData[1], "l"])
                if callCounter>0:
                    list1_high.append(df_15min.at[last15MinIndexTimeData[1], "h"])
            
            if lastIndexTimeData[1] in df.index:
                UnderlyingPrice = df.at[lastIndexTimeData[1], "c"]

            
            # Check for exit conditions and execute exit orders
            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():

                    symSide = row["PositionStatus"]
                    # symSide = symSide[len(symSide) - 2:]      

                    if symSide == -1:
                        if UnderlyingPrice >= (row["entry_price"]+atr_SL):
                            exitType = "MarketStoploss"
                            stockAlgoLogic.exitOrder(index, exitType)
                            list1_low_V = min(list1_low)
                            PutReEntryAllow = True
                            PutEntryAllow = False
                        
                        elif (timeData-900) in df_15min.index:
                            if df_15min.at[last15MinIndexTimeData[1], "EMACross200Above"] == 1:
                                exitType = "EMACross200"
                                stockAlgoLogic.exitOrder(index, exitType)
                                list1_low_V = min(list1_low)
                                PutReEntryAllow = True
                                PutEntryAllow = False


                    elif symSide == 1:
                        if UnderlyingPrice <= (row["entry_price"]-atr_SL):
                            exitType = "MarketStoploss"
                            stockAlgoLogic.exitOrder(index, exitType)
                            list1_high_V = max(list1_high)
                            CallReEntryAllow = True
                            CallEntryAllow = False
                        
                        elif (timeData-900) in df_15min.index:
                            if df_15min.at[last15MinIndexTimeData[1], "EMACross200Below"] == 1:
                                exitType = "EMACross200"
                                stockAlgoLogic.exitOrder(index, exitType)
                                list1_high_V = max(list1_high)
                                CallReEntryAllow = True
                                CallEntryAllow = False


            # Check for entry signals and execute orders
            if ((timeData-900) in df_15min.index) and stockAlgoLogic.openPnl.empty:
                
                if (CallEntryAllow): 
                    if (df_15min.at[last15MinIndexTimeData[1], "c"]> Twoswinghigh):
                        list1_high.append(df_15min.at[last15MinIndexTimeData[1], "h"])
                        logger.info(f"{self.humanTime}\t{self.timeData}\t{stockName}\tclose:{df_15min.at[last15MinIndexTimeData[1], 'c']}")

                        entry_price = df_15min.at[last15MinIndexTimeData[1], "c"]

                        stockAlgoLogic.entryOrder(entry_price, stockName,  (amountPerTrade//entry_price), "BUY", {"entry_price":entry_price},)
                        CallEntryAllow = False
                        maxlist = maxlist[-2:]
                
                if (PutEntryAllow):
                    if df_15min.at[last15MinIndexTimeData[1], "c"]< Twoswinglow:
                        list1_low.append(df_15min.at[last15MinIndexTimeData[1], "l"])
                        logger.info(f"{self.humanTime}\t{self.timeData}\t{stockName}\tclose:{df_15min.at[last15MinIndexTimeData[1], 'c']}")


                        entry_price = df_15min.at[last15MinIndexTimeData[1], "c"]

                        stockAlgoLogic.entryOrder(entry_price, stockName,  (amountPerTrade//entry_price), "SELL", {"entry_price":entry_price},)
                        PutEntryAllow = False
                        lowlist = lowlist[-2:]



                if (PutReEntryAllow): 
                    if df_15min.at[last15MinIndexTimeData[1], "c"]< list1_low_V:
                        logger.info(f"{self.humanTime}\tlist1_low: {list1_low_V}")

                        list1_low.clear()
                        list1_low.append(df_15min.at[last15MinIndexTimeData[1], "l"])

                        entry_price = df_15min.at[last15MinIndexTimeData[1], "c"]

                        stockAlgoLogic.entryOrder(entry_price, stockName,  (amountPerTrade//entry_price), "SELL", {"entry_price":entry_price},)
                        PutReEntryAllow = False  

                if (CallReEntryAllow): 
                    if df_15min.at[last15MinIndexTimeData[1], "c"]> list1_high_V:
                        logger.info(f"{self.humanTime}\tlist1_high: {list1_high_V}\tlist1: {list1_high}")
                        list1_high.clear()
                        list1_high.append(df_15min.at[last15MinIndexTimeData[1], "h"])

                        entry_price = df_15min.at[last15MinIndexTimeData[1], "c"]

                        stockAlgoLogic.entryOrder(entry_price, stockName,  (amountPerTrade//entry_price), "BUY", {"entry_price":entry_price},)
                        CallReEntryAllow = False  

            
            if ((timeData-900) in df_15min.index):
                if MidFlag:
                        Midlist.append(df_15min.at[last15MinIndexTimeData[1], "h"])

            if ((timeData-900) in df_15min.index):
                if MidFlag_low:
                        Midlist_low.append(df_15min.at[last15MinIndexTimeData[1], "l"])

                            

            # if ((timeData-300) in df_15min.index) & (stockAlgoLogic.openPnl.empty) & (stockAlgoLogic.humanTime.time() > time(9, 30)):
            #     if (df_15min.at[last5MinIndexTimeData[1], "c"] > breakp) & (df_15min.at[last5MinIndexTimeData[1], "Scross"] == 1):
            #         entry_price = df_15min.at[last15MinIndexTimeData[1], "c"]

            #         stockAlgoLogic.entryOrder(
            #             entry_price, stockName,  (amountPerTrade//entry_price), "BUY")

        stockAlgoLogic.pnlCalculator()




if __name__ == "__main__":
    startNow = datetime.now()

    # Define Strategy Nomenclature
    devName = "NA"
    # Change 'strategyName' from 'rsiDmiIntraday' to 'rsiDmiOvernight' to switch between strategy
    strategyName = "EquityOvernight"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2024, 1, 2, 9, 15)
    endDate = datetime(2024, 12, 31, 15, 30)
    # endDate = datetime.now()

    portfolio = createPortfolio("/root/Lakshay_Algos/stocksList/nifty50.md",6)

    algoLogicObj = algoLogic(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(
        portfolio, startDate, endDate)


    dailyReport = calculateDailyReport(
        closedPnl, fileDir, timeFrame=timedelta(days=1), mtm=True, fno=False)
    # dailyReport = calculateDailyReport(
    #     closedPnl, fileDir, timeFrame=timedelta(days=1), mtm=True)

    # limitCapital(closedPnl, fileDir, maxCapitalAmount=100000)

    # generateReportFile(dailyReport, fileDir)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")