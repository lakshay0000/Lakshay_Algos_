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

        # # Add 33360 to the index to match the timestamp
        df_1d.index = df_1d.index + 33300
        df_1d.ti = df_1d.ti + 33300

        df_1d = df_1d[df_1d.index >= (startEpoch-(86400*5))]


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_1d.to_csv(
                f"{self.fileDir['backtestResultsCandleData']}{indexName}_1d.csv"
            )
        
        


        lastIndexTimeData = [0, 0]

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()

        Monthlytexpiry = getExpiryData(startEpoch, baseSym)['MonthlyExpiry']
        MonthlytexpiryDatetime = datetime.strptime(Monthlytexpiry, "%d%b%y").replace(hour=15, minute=20)
        MonthlyexpiryEpoch= MonthlytexpiryDatetime.timestamp()

        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        # Skip dates that are not in the allowed list
        allowed_dates = [
            datetime(2021, 2, 1).date(),
            datetime(2022, 2, 1).date(),
            datetime(2023, 2, 1).date(),
            datetime(2024, 2, 1).date(),
            datetime(2024, 7, 23).date(),
            datetime(2025, 2, 1).date(),
        ]       

        otmFactor= 5  #OTM Factor for selecting strike price


        
        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # # Skip the dates 2nd March 2024 and 18th May 2024
            # if self.humanTime.date() == datetime(2024, 1, 5).date():
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
            

            if self.humanTime.date() > expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()

            if self.humanTime.date() > MonthlytexpiryDatetime.date():
                Monthlytexpiry = getExpiryData(self.timeData, baseSym)['MonthlyExpiry']
                MonthlytexpiryDatetime = datetime.strptime(Monthlytexpiry, "%d%b%y").replace(hour=15, minute=20)
                MonthlyexpiryEpoch= MonthlytexpiryDatetime.timestamp()
                

            # if self.humanTime.date() < (expiryDatetime).date():
            #     continue

            # Skip the dates 2nd March 2024 and 18th May 2024
            if self.humanTime.date() not in allowed_dates:
                continue



            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]      

                    if self.humanTime.time() >= time(15, 20):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)



            tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            callCounter= tradecount.get('CE',0)
            putCounter= tradecount.get('PE',0)




            # Check for entry signals and execute orders
            if ((timeData-60) in df.index) and self.humanTime.time() == time(9, 21):

                # weekly Expiry Positions
                callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=otmFactor)

                try:
                    data = self.fetchAndCacheFnoHistData(
                        callSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)

                self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch,},)
                

                putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=otmFactor)

                try:
                    data = self.fetchAndCacheFnoHistData(
                        putSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)


                self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch,},)
                

                # Monthly Expiry Positions
                callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Monthlytexpiry, otmFactor=otmFactor)

                try:
                    data = self.fetchAndCacheFnoHistData(
                        callSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)

                self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": MonthlyexpiryEpoch,},)
                

                putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Monthlytexpiry, otmFactor=otmFactor)

                try:
                    data = self.fetchAndCacheFnoHistData(
                        putSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)


                self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": MonthlyexpiryEpoch,},)





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
    endDate = datetime(2025, 2, 28, 15, 30)

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