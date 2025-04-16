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

    def HedgeTrade(self, date, AtmData, indexPrice, baseSym):
        i=1
        data= None
        while True:
            callsym= self.getCallSym(date, baseSym, indexPrice, otmFactor=i)
            
            try:
                data = self.fetchAndCacheFnoHistData(callsym, date-60)
            except Exception as e:
                self.strategyLogger.info(e)
            
            if data is not None:
                reward = AtmData - data["c"]
                strikedist= i*50
                risk = strikedist - reward
                # Check for risk-reward ratio
                if risk / reward < 1:  
                    i += 1
                else:
                    return i
            else:
                i += 1

            # Break the loop if i exceeds 15
            if i > 15:
                break

        return 0
                

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
            df_3min = getFnoBacktestData(
                indexSym, startEpoch-(86400*50), endEpoch, "3Min")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_3min.dropna(inplace=True)


        results=[]
        results = taa.supertrend(df_3min["h"], df_3min["l"], df_3min["c"], length=100, multiplier=1.8)
        # print(results)
        df_3min["Supertrend1.8"] = results["SUPERTd_100_1.8"]

        results1=[]
        results1 = taa.supertrend(df_3min["h"], df_3min["l"], df_3min["c"], length=100, multiplier=3.6)
        # print(results)
        df_3min["Supertrend3.6"] = results1["SUPERTd_100_3.6"]
        df_3min.dropna(inplace=True)

        # Filter dataframe from timestamp greater than start time timestamp
        df_3min = df_3min[df_3min.index >= startEpoch]

        # # Determine crossover signals
        # df_3min["%KCross80"] = np.where((df_3min["%K"] > 80) & (df_3min["%K"].shift(1) <= 80), 1, 0)
        # df_3min["%KCross20"] = np.where((df_3min["%K"] < 20) & (df_3min["%K"].shift(1) >= 20), 1, 0)
        
        # df_3min["EMACross200"] = np.where((df_3min["EMA200"] > df_3min["c"]) & (df_3min["EMA200"].shift() < df_3min["c"].shift()), 1, 0)
        

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_3min.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_3Min.csv"
        )

        # Strategy Parameters 
        lastIndexTimeData = [0, 0]
        last3MinIndexTimeData = [0, 0]
        StoplossExit=False


        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

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
            if (timeData-180) in df_3min.index:
                last3MinIndexTimeData.pop(0)
                last3MinIndexTimeData.append(timeData-180)

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

            if not self.openPnl.empty and lastIndexTimeData[1] in df.index:
                try:
                    data = self.fetchAndCacheFnoHistData(
                        itmsym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)

                if data["c"] - Stoploss >=5:
                    Stoploss= Stoploss+5
                    self.strategyLogger.info(f"Datetime: {self.humanTime}\t Stoploss updated to {Stoploss} for {itmsym}")
                    lastupdated= self.humanTime.time()

            
            if self.humanTime.date() > (expiryDatetime).date():
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()

            if lastIndexTimeData[1] in df.index:
                UnderlyingPrice = df.at[lastIndexTimeData[1], "c"]



            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    # symstrike = float(row['Symbol'][-7:-2])
      

                    if UnderlyingPrice >= indexprice+(0.005*indexprice) :
                        exitType = "MarketTarget Hit"
                        self.exitOrder(index, exitType)

                    elif row["PositionStatus"] == 1 and row["CurrentPrice"] <= Stoploss:
                        StoplossExit=True

                    elif self.humanTime.time() >= time(15, 20):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)

            if StoplossExit== True:
                for index, row in self.openPnl.iterrows():
                    self.exitOrder(index, f"STOPLOSS HIT AT:- {Stoploss}\t{lastupdated}")
                    StoplossExit= False
    

            # tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            # callCounter= tradecount.get('CE',0)
            # putCounter= tradecount.get('PE',0)

            # Check for entry signals and execute orders
            if ((timeData-180) in df_3min.index) and self.openPnl.empty and self.humanTime.time() < time(15, 20):
                
                if df_3min.at[last3MinIndexTimeData[1], "Supertrend1.8"] == 1 and df_3min.at[last3MinIndexTimeData[1], "Supertrend3.6"] == 1: 
                    callSym = self.getCallSym(
                        self.timeData, baseSym, df_3min.at[last3MinIndexTimeData[1], "c"],expiry= Currentexpiry,otmFactor=1)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    Stoploss = data["c"]
                    itmsym= callSym

                    # Entry Order for ATM
                    callSym_CE = self.getCallSym(
                        self.timeData, baseSym, df_3min.at[last3MinIndexTimeData[1], "c"],expiry= Currentexpiry,otmFactor=0)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym_CE, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    AtmData = data["c"]
                    indexprice = df_3min.at[last3MinIndexTimeData[1], "c"]
                    
                    # Entry Order for Hedge
                    otmFactorH = self.HedgeTrade(self.timeData, AtmData, df_3min.at[last3MinIndexTimeData[1], "c"], baseSym)
                    if otmFactorH != 0:
                        callSym = self.getCallSym(
                            self.timeData, baseSym, df_3min.at[last3MinIndexTimeData[1], "c"],expiry= Currentexpiry,otmFactor=otmFactorH)

                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        self.entryOrder(AtmData, callSym_CE, lotSize, "BUY", {"Expiry": expiryEpoch,},)
                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch,},) 


                # elif NewPosition:
                #     callSym = self.getCallSym(
                #         self.timeData, baseSym, df_3min.at[last3MinIndexTimeData[1], "c"],expiry= Currentexpiry,otmFactor=-1)

                #     try:
                #         data = self.fetchAndCacheFnoHistData(
                #             callSym, lastIndexTimeData[1])
                #     except Exception as e:
                #         self.strategyLogger.info(e)

                #     Stoploss = data["c"]
                #     itmsym= callSym

                #     # Entry Order for ATM
                #     callSym_CE = self.getCallSym(
                #         self.timeData, baseSym, df_3min.at[last3MinIndexTimeData[1], "c"],expiry= Currentexpiry,otmFactor=0)

                #     try:
                #         data = self.fetchAndCacheFnoHistData(
                #             callSym_CE, lastIndexTimeData[1])
                #     except Exception as e:
                #         self.strategyLogger.info(e)

                #     AtmData = data["c"]
                #     indexprice = df_3min.at[last3MinIndexTimeData[1], "c"]
                    
                #     # Entry Order for Hedge
                #     otmFactorH = self.HedgeTrade(self.timeData, AtmData, df_3min.at[last3MinIndexTimeData[1], "c"], baseSym)
                #     if otmFactorH != 0:
                #         callSym = self.getCallSym(
                #             self.timeData, baseSym, df_3min.at[last3MinIndexTimeData[1], "c"],expiry= Currentexpiry,otmFactor=otmFactorH)

                #         try:
                #             data = self.fetchAndCacheFnoHistData(
                #                 callSym, lastIndexTimeData[1])
                #         except Exception as e:
                #             self.strategyLogger.info(e)

                #         self.entryOrder(AtmData, callSym_CE, lotSize, "SELL", {"Expiry": expiryEpoch,},)
                #         self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch,},)

                #     NewPosition = False


                    


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

    # print("Calculating Daily Pnl")
    # dr = calculateDailyReport(
    #     closedPnl, fileDir, timeFrame=timedelta(minutes=5), mtm=True
    # )

    # limitCapital(closedPnl, fileDir, maxCapitalAmount=1000)

    # generateReportFile(dr, fileDir)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")