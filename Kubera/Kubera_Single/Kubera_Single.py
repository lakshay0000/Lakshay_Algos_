import numpy as np
import talib as ta
import pandas_ta as taa
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getEquityBacktestData

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
            df = getEquityBacktestData(indexSym, startEpoch-(86400*50), endEpoch, "1Min")
            df_1d = getEquityBacktestData(indexSym, startEpoch-(86400*500), endEpoch, "1D")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        # df_1d.dropna(inplace=True)

        # Calculate the 20-period EMA
        df['EMA10'] = df['c'].ewm(span=10, adjust=False).mean()
        df_1d['EMA10'] = df_1d['c'].ewm(span=10, adjust=False).mean()   

        # mark candles that break the previous 250-candle high (close > prior 250-high)
        # prev250_high is the rolling max of 'h' over the previous 250 rows (excluded current)
        df_1d['prev250_high'] = df_1d['h'].rolling(window=250, min_periods=250).max().shift(1)
        df_1d['Break250High'] = np.where(df_1d['c'] > df_1d['prev250_high'], 1, 0)
        df_1d['Break250High'].fillna(0, inplace=True) 
        
        # mark candles that break the previous 250-candle low (close < prior 250-low)
        # prev250_low is the rolling min of 'l' over the previous 250 rows (excluded current)
        df_1d['prev250_low'] = df_1d['l'].rolling(window=250, min_periods=250).min().shift(1)
        df_1d['Break250low'] = np.where(df_1d['c'] < df_1d['prev250_low'], 1, 0)
        df_1d['Break250low'].fillna(0, inplace=True) 
        
        df.dropna(inplace=True)

        df = df[df.index >= startEpoch]

        # Determine crossover signals
        df_1d["EMADown"] = np.where((df_1d["EMA10"].shift(1) < df_1d["EMA10"].shift(2)), 1, 0)
        df_1d["EMAUp"] = np.where((df_1d["EMA10"].shift(1) > df_1d["EMA10"].shift(2)), 1, 0)

        df_1d["CloseUp"] = np.where((df_1d["c"].shift(1) > df_1d["c"].shift(2)), 1, 0)

        # Add 33360 to the index to match the timestamp
        df_1d.index = df_1d.index + 33360
        df_1d.ti = df_1d.ti + 33360

        df_1d = df_1d[df_1d.index >= ((startEpoch-86340)-(86400*5))]



        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_1d.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1d.csv"
        )
        



        # Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        # expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        # expiryEpoch= expiryDatetime.timestamp()
        # lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        lastIndexTimeData = [0, 0]
        amountPerTrade = 100000
        TradeLimit = 0
        high_list = []
        low_list = []
        low_list2 =[]
        SL_List = []
        High= None
        Low = None
        Range = None
        Bullish_Day = False
        Bearish_Day = False 
        trailing = False
        new_sl = None
        New_Entry = False
        All_Time_High_Breaks = False
        Last_close = None
        Today_open = None
        Channel_trailing = False



        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # # Skip the dates 2nd March 2024 and 18th May 2024
            if self.humanTime.date() == datetime(2024, 3, 2).date():
                continue

            # Skip time periods outside trading hours
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)

            # Strategy Specific Trading Time
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            if lastIndexTimeData[1] not in df.index:
                self.strategyLogger.info(f"{self.humanTime} {baseSym} Data not found")
                continue


            if (self.humanTime.time() == time(9, 16)):
                O_epoch = timeData
                prev_day = timeData - 86400
                TradeLimit = 0
                high_list = []
                low_list = []
                low_list2 =[]
                High= None
                Low = None
                Range = None
                Bullish_Day = False
                Bearish_Day = False

                #check if previoud day exists in 1d data
                while prev_day not in df_1d.index:
                    prev_day = prev_day - 86400 

                Last_close = df_1d.at[prev_day, "c"] 
                Today_open = df.at[lastIndexTimeData[1], "o"]

                if Last_close >= Today_open:
                    self.strategyLogger.info(f"{self.humanTime} {baseSym} Gap Down detected.")
                    Channel_trailing = False
                
                else:
                    self.strategyLogger.info(f"{self.humanTime} {baseSym} Gap Up detected.")
                    Channel_trailing = True
                    

                if df_1d.at[prev_day, 'Break250High'] == 1:
                    All_Time_High_Breaks = True
                    self.strategyLogger.info(f"{self.humanTime} {baseSym} All Time High Break detected.")


                if trailing == True:
                    SL_List.append(df_1d.at[prev_day, "l"])
                    if len(SL_List) >= 2:
                        if (df_1d.at[timeData, "CloseUp"] == 1):
                            new_sl = SL_List[-1]
                        else:
                            new_sl = new_sl
                    # else:
                    #     new_sl = df_1d.at[prev_day, "l"]

                if New_Entry == False:
                    New_Entry = True


                if df_1d.at[prev_day, 'EMADown'] == 1:
                    Bearish_Day = True
                    Bullish_Day = False
                    self.strategyLogger.info(f"{self.humanTime} {baseSym} Bearish Day detected.")

                elif df_1d.at[prev_day, 'EMAUp'] == 1:
                    Bullish_Day = True
                    Bearish_Day = False
                    self.strategyLogger.info(f"{self.humanTime} {baseSym} Bullish Day detected.")


            if (self.humanTime.time() > time(9, 16)) and (self.humanTime.time() <= time(9, 21)):
                high_list.append(df.at[lastIndexTimeData[1], "h"])
                low_list.append(df.at[lastIndexTimeData[1], "l"])
                if (self.humanTime.time() == time(9, 21)):
                    High = max(high_list)
                    Low = min(low_list)
                    Range = High-Low
                    if Range < 0.002 * (df.at[lastIndexTimeData[1], "o"]):
                        Range = 0.002 * (df.at[lastIndexTimeData[1], "o"])
                        self.strategyLogger.info(f"{self.humanTime} {baseSym} ATR Range too low, setting to 0.2% of open price: {Range}")
                    self.strategyLogger.info(f"{self.humanTime} {baseSym} Range: {Range} High: {High} Low: {Low}")

                    


            # Update current price for open positions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    if lastIndexTimeData[1] in df.index:
                        self.openPnl.at[index,"CurrentPrice"] = df.at[lastIndexTimeData[1], "c"]

            # Calculate and update PnL
            self.pnlCalculator()



            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    # symSide = symSide[len(symSide) - 2:]   
                    # print("open_stock", symSide)  
                    # print("current_stock", stock) 

                    if row["PositionStatus"] == 1:
                        if Bearish_Day:
                            exitType = "Bearish Day Exit"
                            self.exitOrder(index, exitType)
                            trailing = False
                            SL_List.clear()

                        elif df.at[lastIndexTimeData[1], "c"] < df_1d.at[O_epoch, 'prev250_low']:
                            exitType = "All Time Low Break"
                            self.exitOrder(index, exitType)
                            trailing = False
                            SL_List.clear()
                            New_Entry = False
                            All_Time_High_Breaks = False 
                        

                        elif df.at[lastIndexTimeData[1], "c"] < new_sl and row["CurrentPrice"] > row["EntryPrice"]:
                                exitType = "SL Hit"
                                self.exitOrder(index, exitType)
                                trailing = False
                                SL_List.clear()
                                New_Entry = False


                    elif row["PositionStatus"] == -1:

                        if self.humanTime.time() >= time(15, 20):
                            exitType = "Time Up"
                            self.exitOrder(index, exitType)

                        elif df.at[lastIndexTimeData[1], "c"] > High and df.at[lastIndexTimeData[1], "EMA10"] > High:
                            exitType = "Above High Exit"
                            self.exitOrder(index, exitType)




            if self.openPnl.empty and Bullish_Day:
                low_list2.append(df.at[lastIndexTimeData[1], "l"])
            

            if not self.openPnl.empty and Bearish_Day:
                if df.at[lastIndexTimeData[1], 'EMA10'] < Low:
                    Low = df.at[lastIndexTimeData[1], 'EMA10']
                    High = Low + Range
                    self.strategyLogger.info(f"{self.humanTime} {baseSym} New Low: {Low}, High: {High}")


            if (self.humanTime.time() < time(9, 21)):
                continue


            if self.openPnl.empty and Channel_trailing and Bullish_Day:
                if df.at[lastIndexTimeData[1], 'EMA10'] < Low and df.at[lastIndexTimeData[1], 'EMA10'] > Last_close:
                    Low = df.at[lastIndexTimeData[1], 'EMA10']
                    High = Low + Range
                    self.strategyLogger.info(f"{self.humanTime} {baseSym} New Low: {Low}, High: {High}")

            
            # Check for entry signals and execute orders
            if ((timeData-60) in df.index) and self.openPnl.empty and (self.humanTime.time() < time(15, 20)) and New_Entry and All_Time_High_Breaks:
                if High is not None or Low is not None:
                    if Bullish_Day and df.at[lastIndexTimeData[1], "c"] > High:

                        entry_price = df.at[lastIndexTimeData[1], "c"]
                        new_sl = min(low_list2)

                        self.entryOrder(entry_price, baseSym, (amountPerTrade//entry_price), "BUY")
                        trailing = True
                        low_list2.clear()

                    if Bearish_Day and df.at[lastIndexTimeData[1], "c"] < Low and df.at[lastIndexTimeData[1], "EMA10"] < Low and TradeLimit < 3:

                        entry_price = df.at[lastIndexTimeData[1], "c"]

                        self.entryOrder(entry_price, baseSym, (amountPerTrade//entry_price), "SELL")
                        TradeLimit += 1


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
    endDate = datetime(2025, 8, 30, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "BHARATFORG"
    indexName = "BHARATFORG"

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