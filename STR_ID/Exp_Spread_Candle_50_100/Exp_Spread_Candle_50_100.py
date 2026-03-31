import re
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

    def OptChain(self, date, symbol, IndexPrice, baseSym, Strangle_data, otm):
        prev_premium = None
        prev_symbol = None
        nearest_premium = None
        Sym = None
        
        if (symbol == "CE"):
            for i in range(0, otm+1):
                callSymotm = self.getCallSym(date, baseSym, IndexPrice, otmFactor=i, strikeDist=100)         
                try:
                    data = self.fetchAndCacheFnoHistData(callSymotm, date)
                    current_premium = data["c"]
                    
                    # Check if exact match
                    if current_premium == Strangle_data:
                        nearest_premium = current_premium
                        Sym = callSymotm
                        break
                    
                    # If current is less than target
                    if current_premium < Strangle_data:
                        # If first element below target
                        if prev_premium is None:
                            nearest_premium = current_premium
                            Sym = callSymotm
                        else:
                            nearest_premium = prev_premium
                            Sym = prev_symbol
                        break
                    
                    # Store current as previous for next iteration
                    prev_premium = current_premium
                    prev_symbol = callSymotm
                    
                except Exception as e:
                    self.strategyLogger.info(e)
            
            # If loop completed without breaking (all premiums >= target)
            if Sym is None:
                self.strategyLogger.info(f"No premium found below target {Strangle_data}. Selecting closest premium above target.")
                nearest_premium = prev_premium
                Sym = prev_symbol
        
        if (symbol == "PE"):
            for i in range(0, otm+1):
                putSymotm = self.getPutSym(date, baseSym, IndexPrice, otmFactor=i, strikeDist=100)
                try:
                    data = self.fetchAndCacheFnoHistData(putSymotm, date)
                    current_premium = data["c"]
                    
                    # Check if exact match
                    if current_premium == Strangle_data:
                        nearest_premium = current_premium
                        Sym = putSymotm
                        break
                    
                    # If current is less than target
                    if current_premium < Strangle_data:
                        # If first element below target
                        if prev_premium is None:
                            nearest_premium = current_premium
                            Sym = putSymotm
                        else:
                            # Compare which is closer
                            if abs(prev_premium - Strangle_data) <= abs(current_premium - Strangle_data):
                                nearest_premium = prev_premium
                                Sym = prev_symbol
                            else:
                                nearest_premium = current_premium
                                Sym = putSymotm
                        break
                    
                    # Store current as previous for next iteration
                    prev_premium = current_premium
                    prev_symbol = putSymotm
                    
                except Exception as e:
                    self.strategyLogger.info(e)
            
            # If loop completed without breaking (all premiums >= target)
            if Sym is None:
                self.strategyLogger.info(f"No premium found below target {Strangle_data}. Selecting closest premium above target.")
                nearest_premium = prev_premium
                Sym = prev_symbol
        
        self.strategyLogger.info(f"Selected premium: {nearest_premium} Strike: {Sym}")
        
        return Sym, nearest_premium

        

    # Define a method to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):

        # Add necessary columns to the DataFrame
        col = ["Target", "stoploss", "Expiry", "Trailing_Target"]  # Add "Trailing_Flag" if needed
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


        df_1d['EMA10'] = df_1d['c'].ewm(span=10, adjust=False).mean()

        # Determine crossover signals
        df_1d["EMADown"] = np.where((df_1d["EMA10"] < df_1d["EMA10"].shift(1)), 1, 0)
        df_1d["EMAUp"] = np.where((df_1d["EMA10"] > df_1d["EMA10"].shift(1)), 1, 0)

        # Add 33360 to the index to match the timestamp
        df_1d.index = df_1d.index + 33300
        df_1d.ti = df_1d.ti + 33300

        df_1d = df_1d[df_1d.index >= (startEpoch-(86400*5))]


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_1d.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1d.csv")
        


        lastIndexTimeData = [0, 0]

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        Perc = 2  
        Expiry_Day = False
        Square_Off = False



        

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # # Skip the dates 2nd March 2024 and 18th May 2024
            # if self.humanTime.date() == datetime(2025, 4, 7).date() or self.humanTime.date() == datetime(2025, 6, 16).date():
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
                Expiry_Day = True


            if self.humanTime.time() == time(9, 16):
                open = df.at[lastIndexTimeData[1], "o"]


            # if not self.openPnl.empty:
            #     Current_strangle_value = self.openPnl['CurrentPrice'].sum()
            #     open_sum = self.openPnl['Pnl'].sum()
            #     pnnl_sum = sum(pnnl) 
            #     self.strategyLogger.info(f"pnl_sum:{open_sum + pnnl_sum}")

            #     if (open_sum + pnnl_sum) <= -6000:
            #         for index, row in self.openPnl.iterrows():
            #             self.exitOrder(index, "MaxLoss")
            #             EntryAllowed = False
            #             pnnl = []
            #             i = 3
            #             i_CanChange = False                  


            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]  
                    

                    # Exit conditions for CE and PE legs
                    if self.timeData >= row["Expiry"]:
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)

                    elif row["PositionStatus"] == -1:
                        if row["CurrentPrice"] <= row["Target"]:
                            Square_Off = True

                if Square_Off:
                    for index, row in self.openPnl.iterrows():
                        self.exitOrder(index, "Target_Hit")
                    
                    Square_Off = False



            # callCounter= self.openPnl['Symbol'].str[-2:].value_counts().get('CE',0)
            # putCounter= self.openPnl['Symbol'].str[-2:].value_counts().get('PE',0)

            

            # Check for entry signals and execute orders
            if ((timeData-60) in df.index):

                if self.openPnl.empty and self.humanTime.date() != expiryDatetime.date() and self.humanTime.time() > time(15, 20):
                    if self.humanTime.time() == time(15, 25) and Expiry_Day == True:
                        Expiry_Day = False
                        if df.at[lastIndexTimeData[1], "c"] < open:

                            #Straddle Price Calculation
                            callSym, data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, Strangle_data= 100, otm=10)

                            target = 0.2 * data_CE
                            
                            self.entryOrder(data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Target": target},)
                            
                            # For Buying Leg
                            Buy_prm = data_CE/2

                            callSym, data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, Strangle_data = Buy_prm, otm=10)

                            self.entryOrder(data_CE, callSym, lotSize, "BUY", {"Expiry": expiryEpoch},)
                            
                            
                        elif df.at[lastIndexTimeData[1], "c"] >= open:

                            #Straddle Price Calculation
                            putSym, data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, Strangle_data= 100, otm=10)

                            target = 0.2 * data_PE
                            
                            self.entryOrder(data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Target": target},)

                            # For Buying Leg
                            Buy_prm = data_PE/2

                            putSym, data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, Strangle_data= Buy_prm, otm=10)

                            self.entryOrder(data_PE, putSym, lotSize, "BUY", {"Expiry": expiryEpoch},)




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