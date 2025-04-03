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

    # def straddleEntry(self, date, baseSym, expiry, indexPrice, lotSize, symside, positionStatus):
        
    #     if symside == "CE":
    #         TradeSym = self.getCallSym(date, baseSym, indexPrice)
    #     elif symside == "PE":
    #         TradeSym = self.getPutSym(date, baseSym, indexPrice)

    #     try:
    #         data = self.fetchAndCacheFnoHistData(
    #             TradeSym, date)
    #     except Exception as e:
    #         self.strategyLogger.info(e)

    #     self.entryOrder(data["c"], TradeSym, lotSize, positionStatus, {"Expiry": expiry, })

    # Define a method to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):

        # Add necessary columns to the DataFrame
        col = ["Expiry"]
        self.addColumnsToOpenPnlDf(col)

        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            # Fetch historical data for backtesting
            df = getFnoBacktestData(indexSym, startEpoch- 86400*10, endEpoch, "1Min")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)

        # Calculate RSI indicator
        df["rsi"] = ta.RSI(df["c"], timeperiod=14)
        df.dropna(inplace=True)

        # Filter dataframe from timestamp greater than start time timestamp
        df = df[df.index > startEpoch]

        # Determine crossover signals
        df["rsiCross70"] = np.where(
            (df["rsi"] > 70) & (df["rsi"].shift(1) <= 70), 1, 0)
        df["rsiCross40"] = np.where(
            (df["rsi"] < 40) & (df["rsi"].shift(1) >= 40), 1, 0)
        
        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        

        # Strategy Parameters       
        lastIndexTimeData = [0, 0]
        BuyEntry=True
        SellEntry=True

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

            # Strategy Specific Trading Time
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            # Log relevant information
            # if (timeData-300).index:
            #     self.strategyLogger.info(
            #         f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}\trsi60: {df.at[last5MinIndexTimeData[1],'rsiCross60']}\trsi40: {df.at[last5MinIndexTimeData[1],'rsiCross40']}")

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

            if self.humanTime.date() > expiryDatetime.date() :
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
                

            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]
      

                    if (df.at[lastIndexTimeData[1], "rsiCross40"] == 1) and (row["PositionStatus"] == 1):  
                            exitType = "RSIbreak40"
                            self.exitOrder(index, exitType)
                            SellEntry=True
                    elif (df.at[lastIndexTimeData[1], "rsiCross70"] == 1) and (row["PositionStatus"] == -1):
                            exitType = "RSIbreak70"
                            self.exitOrder(index, exitType)
                            BuyEntry=True

                    elif self.timeData >= row["Expiry"]:
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)
                        BuyEntry=True
                        SellEntry=True
    

            # Check for entry signals and execute orders
            if (lastIndexTimeData[1] in df.index) and self.humanTime.date() == expiryDatetime.date():
                
                if df.at[lastIndexTimeData[1], "rsiCross40"] == 1 and SellEntry:
                    callSym = self.getCallSym(
                        self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch, })

                    putSym = self.getPutSym(
                        self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, },)
                    SellEntry=False

                if df.at[lastIndexTimeData[1], "rsiCross70"] == 1 and BuyEntry:
                    callSym = self.getCallSym(
                        self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch, })

                    putSym = self.getPutSym(
                        self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch, },)
                    BuyEntry=False




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
    startDate = datetime(2023, 1, 1, 9, 15)
    endDate = datetime(2023, 1, 25, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    # Execute the algorithm
    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculateDailyReport(
        closedPnl, fileDir, timeFrame=timedelta(minutes=5), mtm=True
    )

    limitCapital(closedPnl, fileDir, maxCapitalAmount=1000)

    generateReportFile(dr, fileDir)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")