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
            df = getEquityBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            df_1d = getEquityBacktestData(indexSym, startEpoch-(86400*10), endEpoch, "1D")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        # df_1d.dropna(inplace=True)

        # Calculate the 20-period EMA
        # df['EMA10'] = df['c'].ewm(span=10, adjust=False).mean()
        
        df_1d.dropna(inplace=True)

        # df = df[df.index >= startEpoch]

        # # Determine crossover signals
        # df_1d["EMADown"] = np.where((df_1d["EMA10"].shift(1) < df_1d["EMA10"].shift(2)), 1, 0)
        # df_1d["EMAUp"] = np.where((df_1d["EMA10"].shift(1) > df_1d["EMA10"].shift(2)), 1, 0)

        # df_1d["CloseUp"] = np.where((df_1d["c"].shift(1) > df_1d["c"].shift(2)), 1, 0)

        # # Add 33360 to the index to match the timestamp
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
        open_price = None
        exit_signal = False



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

            # Strategy Specific Trading Time
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            if lastIndexTimeData[1] not in df.index:
                self.strategyLogger.info(f"{self.humanTime} Data not found for {baseSym} at index {lastIndexTimeData[1]}")


            if (self.humanTime.time() == time(9, 16)):
                open_price = df.at[lastIndexTimeData[1], "o"]
                self.strategyLogger.info(f"{self.humanTime} {baseSym} Open Price: {open_price}")
                
                prev_day = timeData - 86400
                #check if previoud day exists in 1d data
                while prev_day not in df_1d.index:
                    prev_day = prev_day - 86400 


            # if (self.humanTime.time() > time(9, 16)) and (self.humanTime.time() <= time(9, 21)):
            #     high_list.append(df.at[lastIndexTimeData[1], "h"])
            #     low_list.append(df.at[lastIndexTimeData[1], "l"])
            #     if (self.humanTime.time() == time(9, 21)):
            #         High = max(high_list)
            #         Low = min(low_list)
            #         Range = High-Low
            #         if Range < 0.002 * (df.at[lastIndexTimeData[1], "o"]):
            #             Range = 0.002 * (df.at[lastIndexTimeData[1], "o"])
            #             self.strategyLogger.info(f"{self.humanTime} {baseSym} ATR Range too low, setting to 0.2% of open price: {Range}")
            #         self.strategyLogger.info(f"{self.humanTime} {baseSym} Range: {Range} High: {High} Low: {Low}")

                    


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

                    if self.humanTime.time() >= time(15, 20) and self.humanTime.date() != row["EntryTime"].date() and exit_signal == False:
                        if df.at[lastIndexTimeData[1], "c"] < open_price and (df.at[lastIndexTimeData[1], "c"] < df_1d.at[prev_day, "l"]):
                            exit_signal=True
                            self.strategyLogger.info(f"{self.humanTime} {baseSym} Exit Signal Triggered: Time Up and Price below Open Price")

                        elif (df.at[lastIndexTimeData[1], "c"] > open_price) and (df.at[lastIndexTimeData[1], "c"] > df_1d.at[prev_day, "h"]):
                            self.strategyLogger.info(f"{self.humanTime} {baseSym} Exit Signal Triggered: Time Up and Price above Open Price")
                            exitType = "Traget Green Candle Exit"
                            self.exitOrder(index, exitType)
                            exit_signal = False
                        


                    if exit_signal == True and row['CurrentPrice'] >= row['EntryPrice']:
                        exitType = "Red Candle Breakeven Exit"
                        self.exitOrder(index, exitType)
                        exit_signal = False

            
            # Check for entry signals and execute orders
            if ((timeData-60) in df.index) and self.openPnl.empty and (self.humanTime.time() >= time(15, 20)):
                if (df.at[lastIndexTimeData[1], "c"] > open_price) and (df.at[lastIndexTimeData[1], "c"] > df_1d.at[prev_day, "h"]):

                    entry_price = df.at[lastIndexTimeData[1], "c"]
                    
                    self.strategyLogger.info(f"{self.humanTime} {baseSym} Entry Signal Triggered: Green Candle Entry at {entry_price}")
                    self.entryOrder(entry_price, baseSym, (amountPerTrade//entry_price), "BUY")




        # # At the end of the trading day, exit all open positions
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
    startDate = datetime(2025, 1, 1, 9, 15)
    endDate = datetime(2025, 11, 30, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "RELIANCE"
    indexName = "RELIANCE"

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