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
            df_1d = getFnoBacktestData(indexSym, startEpoch-(86400*50), endEpoch, "1D")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_1d.dropna(inplace=True)

        # Add 33360 to the index to match the timestamp
        df_1d.index = df_1d.index + 33360
        df_1d.ti = df_1d.ti + 33360

        df_1d = df_1d[df_1d.index >= startEpoch-86340]


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_1d.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1d.csv"
        )
        


        lastIndexTimeData = [0, 0]

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        m_upper = None
        m_lower = None
        i=0

        

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # # Skip the dates 2nd March 2024 and 18th May 2024
            # if self.humanTime.date() == datetime(2024, 3, 2).date() or self.humanTime.date() == datetime(2024, 2, 15).date():
            #     continue

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
                continue

            #  # Log relevant information
            # if lastIndexTimeData[1] in df.index:
            #     self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")

            if (self.humanTime.time() == time(9, 16)):
                m_upper = None
                m_lower = None
                i=0


            # Update current price for open positions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    if lastIndexTimeData[1] in df.index:
                        self.openPnl.at[index,"CurrentPrice"] = df.at[lastIndexTimeData[1], "c"]

            # Calculate and update PnL
            self.pnlCalculator()
            


            #Updating daily index
            prev_day = timeData - 86400
            if timeData in df_1d.index:
                Today_open = df.at[lastIndexTimeData[1], 'o']
                #check if previoud day exists in 1d data
                while prev_day not in df_1d.index:
                    prev_day = prev_day - 86400

            if prev_day in df_1d.index:
                prev_DH = (df_1d.at[prev_day, 'h'])
                prev_DL = (df_1d.at[prev_day, 'l'])  
                self.strategyLogger.info(f"{self.humanTime} Previous Day High: {prev_DH}, Previous Day Low: {prev_DL}, Today Open: {Today_open}, BarNo: {375 + i}")

            if m_upper is None and m_lower is None:
                m_upper = (Today_open - prev_DH) / 375
                m_lower = (Today_open - prev_DL) / 375
                self.strategyLogger.info(f"{self.humanTime} Slope Upper: {m_upper}, Slope Lower: {m_lower}")

            if lastIndexTimeData[1] in df.index:
                BarNo = 375 + i
                upper_ray = prev_DH + (m_upper * BarNo)
                lower_ray = prev_DL + (m_lower * BarNo)
                i= i + 1
                self.strategyLogger.info(f"{self.humanTime} Upper Ray: {upper_ray}, Lower Ray: {lower_ray}, BarNo: {BarNo}")


            

            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    # symSide = row["Symbol"]
                    # symSide = symSide[len(symSide) - 2:]      
                        

                    if self.humanTime.time() >= time(15, 20):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)

                    elif (row["PositionStatus"]==1) and df.at[lastIndexTimeData[1], "c"] < upper_ray:
                        exitType = "Upper Ray Hit"
                        self.exitOrder(index, exitType)

                        entry_price = df.at[lastIndexTimeData[1], "c"]

                        self.entryOrder(entry_price, "NIFTY50", lotSize, "SELL")


                    elif (row["PositionStatus"]==-1) and df.at[lastIndexTimeData[1], "c"] > lower_ray:
                        exitType = "Lower Ray Hit"
                        self.exitOrder(index, exitType)
                        entry_price = df.at[lastIndexTimeData[1], "c"]

                        self.entryOrder(entry_price, "NIFTY50", lotSize, "BUY")



            # Check for entry signals and execute orders
            if ((timeData-60) in df.index) and self.openPnl.empty and (self.humanTime.time() == time(9, 18)):

                if df.at[lastIndexTimeData[1], "c"] > lower_ray:

                    entry_price = df.at[lastIndexTimeData[1], "c"]

                    self.entryOrder(entry_price, "NIFTY50", lotSize, "BUY")
                    
                
                if df.at[lastIndexTimeData[1], "c"] < upper_ray:

                    entry_price = df.at[lastIndexTimeData[1], "c"]


                    self.entryOrder(entry_price, "NIFTY50", lotSize, "SELL")



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
    startDate = datetime(2020, 4, 9, 9, 15)
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