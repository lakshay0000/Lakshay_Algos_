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
    
    def round_to_nearest(self, number, distance):
        
        a= round(number / distance) * distance
        return a
    

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


        lastIndexTimeData = [0, 0]
        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        NextExpiry = getExpiryData(startEpoch, baseSym)['NextExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        for timeData in df.index:
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)

            self.timeData = timeData
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # Skip the dates 2nd March 2024 and 18th May 2024
            if self.humanTime.date() in [datetime(2024, 3, 2).date(), datetime(2024, 5, 18).date()]:
                continue

           #skip times period other than trading hours than ()
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue


            # Update current price for open positions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)
            
             # Log relevant information
            if lastIndexTimeData[1] in df.index:
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")


            # Calculate and update PnL
            self.pnlCalculator()

            if self.humanTime.date() > expiryDatetime.date() :
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']
                NextExpiry = getExpiryData(self.timeData, baseSym)['NextExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)


            if self.openPnl.shape[0] == 4:
                sell_positions = self.openPnl[self.openPnl["PositionStatus"] == -1]
                # Separate CE and PE based on the symbol
                ce_positions = sell_positions[sell_positions["Symbol"].str.contains("CE")]
                pe_positions = sell_positions[sell_positions["Symbol"].str.contains("PE")]                
                
                # Check if ce_positions and pe_positions are not empty
                if not ce_positions.empty and not pe_positions.empty:
                # Calculate CE and PE total values
                    ce_value = ce_positions["CurrentPrice"].iloc[0]
                    pe_value = pe_positions["CurrentPrice"].iloc[0]
                    # self.strategyLogger.info(f"ce_value:{ce_value}\tpe_value:{pe_value}"))

                    ratio = ce_value + pe_value
                    self.strategyLogger.info(f"ratio:{ratio}\tce_value:{ce_value}\tpe_value:{pe_value}")
                else:
                    self.strategyLogger.info("CE or PE positions are empty, skipping ratio calculation.")



            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty and self.openPnl.shape[0] == 4:
                for index, row in self.openPnl.iterrows():

                    if self.humanTime.time() == time(15, 1): 
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)  

                    elif 'ratio' in locals() and ratio > otmtrade:
                        exitType = "Stoploss"
                        self.exitOrder(index, exitType)



            # Entry Order for Callender Spread
            if (lastIndexTimeData[1] in df.index) and (self.humanTime.time()== time(9, 16)):
                callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry=Currentexpiry, otmFactor=0)
                try:
                    data = self.fetchAndCacheFnoHistData(
                        callSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)

                self.entryOrder(data["c"], callSym, lotSize, "SELL")


                putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry=Currentexpiry, otmFactor=0)
                try:
                    data1 = self.fetchAndCacheFnoHistData(
                        putSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)
                
                self.entryOrder(data1["c"], putSym, lotSize, "SELL")
                
                Atmsum = data["c"] + data1["c"]
                otmtrade= Atmsum*1.25
                strkdist= self.round_to_nearest(otmtrade, 50)
                otmF= strkdist/50 

                callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry=NextExpiry, otmFactor=otmF)
                try:
                    data = self.fetchAndCacheFnoHistData(
                        callSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)

                self.entryOrder(data["c"], callSym, lotSize, "BUY")

                putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry=NextExpiry, otmFactor=otmF)
                try:
                    data = self.fetchAndCacheFnoHistData(
                        putSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)
                
                self.entryOrder(data["c"], putSym, lotSize, "BUY")



        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]



if __name__ == "__main__":
    startTime = datetime.now()

    # Define Strategy Nomenclature
    devName = "NA"
    strategyName = "0vernight3"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2024, 12, 30, 15, 30)

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