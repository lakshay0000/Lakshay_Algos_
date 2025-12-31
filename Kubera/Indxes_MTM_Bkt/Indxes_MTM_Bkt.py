import numpy as np
import pandas as pd
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

        # try:
        #     # Fetch historical data for backtesting
        #     df = getEquityBacktestData(indexSym, startEpoch, endEpoch, "1Min")
        #     # df_1d = getEquityBacktestData(indexSym, startEpoch-(86400*50), endEpoch, "1D")
        # except Exception as e:
        #     # Log an exception if data retrieval fails
        #     self.strategyLogger.info(
        #         f"Data not found for {baseSym} in range {startDate} to {endDate}")
        #     raise Exception(e)
        
        df = pd.read_csv("/root/Lakshay_Algos/Kubera/Json/mtm_Lakshay_Algos_Kubera_Json.csv")
        df['Date'] = pd.to_datetime(df['Date'])

        # Add epoch column (seconds)
        df['ti'] = df['Date'].astype('int64') // 10**9
        df.set_index('ti', inplace=True)


        # Drop rows with missing values
        df.dropna(inplace=True)

        # # Add 33360 to the index to match the timestamp
        df.index = df.index - 19800

        # df_1d = df_1d[df_1d.index >= startEpoch-86340]


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        


        lastIndexTimeData = [0, 0]

        # Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        # expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        # expiryEpoch= expiryDatetime.timestamp()
        # lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        m_upper = None
        m_lower = None
        i=0
        k=0
        amountPerTrade = 100000
        MainTrade=True


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

            # if lastIndexTimeData[1] not in df.index:
            #     continue

            #  # Log relevant information
            # if lastIndexTimeData[1] in df.index:
            #     self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")

            if (self.humanTime.time() == time(9, 16)):
                MainTrade=True


            # Update current price for open positions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    if lastIndexTimeData[1] in df.index:
                        self.openPnl.at[index,"CurrentPrice"] = df.at[lastIndexTimeData[1], "mtmPnl"]

            # Calculate and update PnL
            self.pnlCalculator()

            
            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    # symSide = row["Symbol"]
                    # symSide = symSide[len(symSide) - 2:]      
                        

                    if self.humanTime.time() >= time(15, 20):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)
                        MidDay=False

                    elif df.at[lastIndexTimeData[1], "mtmPnl"] < -1000:
                        exitType = "stoploss"
                        self.exitOrder(index, exitType)
                        MidDay=False


            # Check for entry signals and execute orders

            if ((timeData-60) in df.index) and self.openPnl.empty and (self.humanTime.time() < time(15, 20)):

                if MainTrade:
                    entry_price = df.at[lastIndexTimeData[1], "mtmPnl"]
                    self.strategyLogger.info(f"Placing Main Trade at {entry_price}")
                    self.entryOrder(entry_price, baseSym, 1, "BUY")
                    MainTrade=False

                elif not MainTrade and df.at[lastIndexTimeData[1], "mtmPnl"] > 0:
                    
                    entry_price = df.at[lastIndexTimeData[1], "mtmPnl"]

                    self.entryOrder(entry_price, baseSym, 1, "BUY")



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
    baseSym = "EquityMTM"
    indexName = "EquityMTM"

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