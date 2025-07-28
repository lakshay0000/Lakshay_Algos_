import numpy as np
import talib as ta
import re
import pandas_ta as taa
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getFnoBacktestData

# sys.path.insert(1, '/root/backtestTools')


# Define a class algoLogic that inherits from optOverNightAlgoLogic
class algoLogic(optOverNightAlgoLogic):

    def extract_strike(self, symbol):
        # Match 2 digits (year), then capture the strike before CE/PE
        match = re.search(r'\d{2}[A-Z]{3}\d{2}(\d+)(CE|PE)', symbol)
        if match:
            return int(match.group(1))
        else:
            return None

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

        Currentexpiry = getExpiryData(startEpoch, baseSym)['MonthlyExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        # lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # Skip the dates 2nd March 2024 and 18th May 2024
            if self.humanTime.date() in [datetime(2024, 3, 2).date(), datetime(2024, 5, 18).date()]:
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

            # # Log relevant information
            # if (timeData-300) in df_1h.index:
            #     self.strategyLogger.info(
            #         f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}\trsi60: {df_1h.at[last5MinIndexTimeData[1],'rsiCross60']}\trsi50: {df_1h.at[last5MinIndexTimeData[1],'rsiCross50']}\trsi40: {df_1h.at[last5MinIndexTimeData[1],'rsiCross40']}")

            # Update current price for open positions
            # if not self.openPnl.empty:
            #     for index, row in self.openPnl.iterrows():
            #         try:
            #             data = self.fetchAndCacheFnoHistData(
            #                 row["Symbol"], lastIndexTimeData[1])
            #             self.openPnl.at[index, "CurrentPrice"] = data["c"]
            #         except Exception as e:
            #             self.strategyLogger.info(e)

            # Calculate and update PnL
            self.pnlCalculator()

            if self.humanTime.date() >= (expiryDatetime).date():
                Currentexpiry = getExpiryData(self.timeData+(86400), baseSym)['MonthlyExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()

            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    Symbol = row["Symbol"]   
                    symSide = Symbol[len(Symbol) - 2:]   

                    if self.timeData >= row["Expiry"]:
                        strike = self.extract_strike(Symbol)
                        UnderlyingPrice = df.at[lastIndexTimeData[1], "c"]
                        if symSide == "CE":
                            if UnderlyingPrice >= strike:
                                exitPrice = round(UnderlyingPrice-strike, 2)
                            else:
                                exitPrice = 0.1

                            exitType = f"Time Up {UnderlyingPrice}"
                            self.exitOrder(index, exitType, exitPrice)

                        elif symSide == "PE":
                            if UnderlyingPrice <= strike:
                                exitPrice = round(strike-UnderlyingPrice, 2)
                            else:
                                exitPrice = 0.1

                            exitType = f"Time Up {UnderlyingPrice}"
                            self.exitOrder(index, exitType, exitPrice)

    
            # Check for entry signals and execute orders
            if ((timeData-60) in df.index) and self.openPnl.empty and self.humanTime.time() == time(15, 20):

                UnderlyingPrice = df.at[lastIndexTimeData[1], "c"]
                lotSize=  round((10000000/ UnderlyingPrice))

                self.strategyLogger.info(f"{self.humanTime}\t{self.timeData}\t{baseSym}\tclose:{df.at[lastIndexTimeData[1], 'c']}\texpiry: {Currentexpiry}")
                putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)
                self.strategyLogger.info(f"{self.humanTime}\tputSym: {putSym}")

                try:
                    data = self.fetchAndCacheFnoHistData(
                        putSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)


                self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch,})
                    
                # call buy
                callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)
                self.strategyLogger.info(f"{self.humanTime}\tcallSym: {callSym}")
                try:
                    data = self.fetchAndCacheFnoHistData(
                        callSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)


                self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch,})
                


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
    startDate = datetime(2024, 1, 3, 9, 15)
    endDate = datetime(2024, 12, 31, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "BANKNIFTY"
    indexName = "NIFTY BANK"

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