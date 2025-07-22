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
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}") 
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")


        # Strategy Parameters

        lastIndexTimeData = [0, 0]


        Currentexpiry = getExpiryData(startEpoch, baseSym, conn=conn)['MonthlyExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        StrikeDist = int(getExpiryData(startEpoch, baseSym, conn=conn)["StrikeDist"])
        # lotSize = int(getExpiryData(startEpoch, baseSym)["LotSize"])


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

            #  # Log relevant information
            # if lastIndexTimeData[1] in df.index:
            #     self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")



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
            

            if self.humanTime.date() >= (expiryDatetime).date():
                Currentexpiry = getExpiryData(self.timeData+(86400), baseSym, conn=conn)['MonthlyExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
                StrikeDist = int(getExpiryData(startEpoch, baseSym, conn=conn)["StrikeDist"])            
            
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


            # tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            # callCounter= tradecount.get('CE',0)
            # putCounter= tradecount.get('PE',0)

            # Check for entry signals and execute orders
            if ((timeData-60) in df.index) and self.openPnl.empty:

                UnderlyingPrice = df.at[lastIndexTimeData[1], "c"]
                lotSize=  (10000000/ UnderlyingPrice)* 32

                self.strategyLogger.info(f"{self.humanTime}\t{self.timeData}\t{baseSym}\tclose:{df.at[lastIndexTimeData[1], 'c']}\texpiry: {Currentexpiry}")
                putSym = self.getPutSym(
                    self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry,strikeDist= StrikeDist, conn=conn)
                self.strategyLogger.info(f"{self.humanTime}\tputSym: {putSym}")

                try:
                    data = getEquityHistData(
                        putSym, lastIndexTimeData[1], conn=conn)
                except Exception as e:
                    self.strategyLogger.info(e)

                self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch},)

                # call sell
                callSym = self.getCallSym(
                    self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry,strikeDist= StrikeDist, conn=conn)
                self.strategyLogger.info(f"{self.humanTime}\tcallSym: {callSym}")

                try:
                    data = getEquityHistData(
                        callSym, lastIndexTimeData[1], conn=conn)
                except Exception as e:
                    self.strategyLogger.info(e)


                self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)



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
    baseSym = "HDFCBANK"
    indexName = "HDFCBANK"

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