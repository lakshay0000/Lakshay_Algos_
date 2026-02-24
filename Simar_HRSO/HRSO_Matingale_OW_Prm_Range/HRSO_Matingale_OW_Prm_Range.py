import numpy as np
import talib as ta
import pandas_ta as taa
import pandas as pd
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getFnoBacktestData

# sys.path.insert(1, '/root/backtestTools')


# Define a class algoLogic that inherits from optOverNightAlgoLogic
class algoLogic(optOverNightAlgoLogic):

    def findValidOptionSymbol(self, symbolType, IndexPrice, entry_price, baseSym, lastIndexTimeData, Currentexpiry, otmFactor_start):
            """
            Find a valid option symbol with price in range [50, 150].
            
            Args:
                symbolType: "CALL" or "PUT"
                entry_price: Current entry price
                baseSym: Base symbol (e.g., "NIFTY")
                lastIndexTimeData: Last index time data
                Currentexpiry: Current expiry date
                otmFactor_start: Starting OTM factor (-1)
            
            Returns:
                tuple: (symbol, data["c"]) or (None, None) if not found
            """
            i = otmFactor_start
            max_iterations = 20  # Prevent infinite loops
            iterations = 0
            
            direction = -1 if entry_price < 100 else 1
            
            while iterations < max_iterations:
                try:
                    if symbolType == "CALL":
                        symbol = self.getCallSym(self.timeData, baseSym, IndexPrice, expiry=Currentexpiry, otmFactor=i)

                    else:  # PUT
                        symbol = self.getPutSym(self.timeData, baseSym, IndexPrice, expiry=Currentexpiry, otmFactor=i)
                    
                    data = self.fetchAndCacheFnoHistData(symbol, lastIndexTimeData[1])
                    
                    if 100 <= data["c"] <= 300:
                        return symbol, data["c"]
                    
                    i += direction
                    iterations += 1
                    
                except Exception as e:
                    self.strategyLogger.info(f"Error fetching {symbolType} data: {e}")
                    i += direction
                    iterations += 1
            
            self.strategyLogger.info(f"No valid {symbolType} symbol found in {max_iterations} iterations")
            return None, None

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
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        df_CE = None
        df_1d_CE = None
        df_PE = None
        df_1d_PE = None

        CE_Target = False
        PE_Target = False

        PE_High = 70
        PE_Low = 30

        CE_High = 70
        CE_Low = 30

        PE_Ls = 1
        CE_Ls = 1
        MaxLoss_Hit = False
        New_Call_Entry = False
        New_Put_Entry = False
        callSym = None
        putSym = None

        otmfactor = -1



        

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
                df_CE = None  # Reset for next day
                df_PE = None  # Reset for next day
                callSym = None
                putSym = None


                if PE_Target and CE_Target:
                    PE_Ls=1
                    CE_Ls=1

                elif PE_Target:
                    if PE_Ls == 2:
                        if CE_Ls<2:
                            CE_Ls = PE_Ls
                    else:
                        PE_Ls = PE_Ls*2
                    self.strategyLogger.info(f"{self.humanTime} PE Lotsize updated = {lotSize*PE_Ls}")

                elif CE_Target:
                    if CE_Ls == 2:
                        if PE_Ls<2:
                            PE_Ls = CE_Ls
                    else:
                        CE_Ls = CE_Ls*2
                    self.strategyLogger.info(f"{self.humanTime} PE Lotsize updated = {lotSize*CE_Ls}")
                
                # Set flags true for next expiry to double the lot size
                CE_Target = False
                PE_Target = False
                MaxLoss_Hit = False


                

            if self.humanTime.date() < (expiryDatetime).date():
                continue


            if self.humanTime.time() >= time(9, 17) and self.humanTime.time() < time(15, 20):
                if self.humanTime.time() == time(9, 17):
                    open_epoch = lastIndexTimeData[1]
                    self.strategyLogger.info(f"{self.humanTime} otmFactor={otmfactor}")
                
                # Fetch Call DataFrame separately
                if df_CE is None:
                    try:
                        if callSym is None:
                            callSym = self.getCallSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=otmfactor)
                        
                        df_CE = getFnoBacktestData(callSym, open_epoch, open_epoch + 86400, "1Min")
                        df_CE['High'] = df_CE['h'].cummax()
                        df_CE['Low'] = df_CE['l'].cummin()
                        df_CE['range'] = df_CE['High'] - df_CE['Low']
                        df_CE['HRSO'] = ((df_CE['c'] - df_CE['Low']) / df_CE['range'])*100
                        self.strategyLogger.info(f"{self.humanTime} {callSym} df_CE loaded successfully")
                        self.strategyLogger.info(f"{self.humanTime} {callSym} df_CE:\n{df_CE.head(350).to_string()}")

                        
                    except Exception as e:
                        self.strategyLogger.info(f"Failed to fetch CE data at {self.humanTime} {callSym}: {e}")
                        df_CE = None
                        callSym = None
                
                # Fetch Put DataFrame separately
                if df_PE is None:
                    try:
                        if putSym is None:
                            putSym = self.getPutSym(
                                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=otmfactor)
                        
                        df_PE = getFnoBacktestData(putSym, open_epoch, open_epoch + 86400, "1Min")
                        df_PE['High'] = df_PE['h'].cummax()
                        df_PE['Low'] = df_PE['l'].cummin()
                        df_PE['range'] = df_PE['High'] - df_PE['Low']
                        df_PE['HRSO'] = ((df_PE['c'] - df_PE['Low']) / df_PE['range'])*100
                        self.strategyLogger.info(f"{self.humanTime} {putSym} df_PE loaded successfully")
                        self.strategyLogger.info(f"{self.humanTime} {putSym} df_PE:\n{df_PE.head(350).to_string()}")
                        
                    except Exception as e:
                        self.strategyLogger.info(f"Failed to fetch PE data at {self.humanTime} {putSym}: {e}")
                        df_PE = None
                        putSym = None

                
                # pe_prev_day_high = df_1d_PE['h'].max()
                # pe_prev_day_low = df_1d_PE['l'].min()
                # ce_prev_day_high = df_1d_CE['h'].max()
                # ce_prev_day_low = df_1d_CE['l'].min()
                # self.strategyLogger.info(f"pe_prev_day_high:{pe_prev_day_high}")
                # self.strategyLogger.info(f"pe_prev_day_low:{pe_prev_day_low}")
                # self.strategyLogger.info(f"ce_prev_day_high:{ce_prev_day_high}")
                # self.strategyLogger.info(f"ce_prev_day_low:{ce_prev_day_low}")


            # Check if the current time is past the expiry time
            # if prev_day is None:
            #     prev_day = expiryEpoch - 86400
            #     if timeData in df.index:
            #         #check if previoud day exists in 1d data
            #         while prev_day not in df.index:
            #             prev_day = prev_day - 86400                
            

            # if lastIndexTimeData[1] in df.index:
            #     try:
            #         # for call
            #         data = self.fetchAndCacheFnoHistData(St_CallSym, lastIndexTimeData[1])
            #         a= data["c"]
            #         # for put 
            #         data = self.fetchAndCacheFnoHistData(St_PutSym, lastIndexTimeData[1])
            #         b= data["c"]
            #         Current_strangle_value = a + b
            #     except Exception as e:
            #         self.strategyLogger.info(e)
            
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


            # # First, check all positions for stoploss
            # if not self.openPnl.empty and (Stranggle_Exit == False):
            #     for index, row in self.openPnl.iterrows():
            #         if row["CurrentPrice"] >= row["Stoploss"]:
            #             Stranggle_Exit = True
            #             if i_CanChange:
            #                 if i < 5:
            #                     i += 1
            #                     self.strategyLogger.info(f"i value increased to {i}")
            #                 else:
            #                     i = 5
            #                     self.strategyLogger.info(f"i value remains {i}")
            #                 i_CanChange = False
            
            # if self.humanTime.time() > time(9, 17):
            #     if (lastIndexTimeData[1] in df_CE.index):


            # if not self.openPnl.empty:
            #     open_sum = int(self.openPnl['Pnl'].sum())

            #     self.closedPnl['ExitTime'] = pd.to_datetime(self.closedPnl['ExitTime'])
            #     currentDayClosedPnl = self.closedPnl[self.closedPnl['ExitTime'].dt.date == self.humanTime.date()]
            #     close_sum = int(currentDayClosedPnl['Pnl'].sum())

            #     self.strategyLogger.info(f"{self.humanTime} pnl_sum:{open_sum + close_sum}")

            #     if (open_sum + close_sum) < -10000:
            #         for index, row in self.openPnl.iterrows():
            #             self.exitOrder(index, "MaxLoss")

            #         MaxLoss_Hit = True



            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]      

                    if self.humanTime.time() >= time(15, 20):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)

                    elif symSide == 'CE':
                        if CE_Target == False and row["CurrentPrice"] <= row["EntryPrice"]*0.5:
                            self.openPnl.at[index, "stoploss"] = row["EntryPrice"]
                            self.strategyLogger.info(f"{self.humanTime} stoploss shifted to breakeven: {self.openPnl.at[index, 'stoploss']}")
                            self.strategyLogger.info(f"{self.openPnl[['Symbol', 'Target', 'stoploss']].to_string()}")

                        if row["CurrentPrice"] <= row["Target"]:
                            self.openPnl.at[index, "stoploss"] = (df_CE.loc[:lastIndexTimeData[1]-60, 'c'].min())*2
                            self.openPnl.at[index, "Target"] = row["CurrentPrice"]
                            self.strategyLogger.info(f"{self.humanTime} TARGET HIT CE and stoploss shifted to: {self.openPnl.at[index, 'stoploss']}")
                            self.strategyLogger.info(f"Target: {self.openPnl.at[index, 'Target']}")
                            self.strategyLogger.info(f"{self.openPnl[['Symbol', 'Target', 'stoploss']].to_string()}")
                            CE_Target = True


                        elif row["CurrentPrice"] >= row["stoploss"]:
                            exitType = "CE_Stoploss Hit"
                            self.exitOrder(index, exitType)

                        elif (lastIndexTimeData[1] in df_CE.index):
                            if df_CE.at[lastIndexTimeData[1], "HRSO"] > CE_High:
                                exitType = "CE_high_Break"
                                self.exitOrder(index, exitType)

                        
                    elif symSide == 'PE':
                        if PE_Target == False and row["CurrentPrice"] <= row["EntryPrice"]*0.5:
                            self.openPnl.at[index, "stoploss"] = row["EntryPrice"]
                            self.strategyLogger.info(f"{self.humanTime} stoploss shifted to breakeven: {self.openPnl.at[index, 'stoploss']}")
                            self.strategyLogger.info(f"{self.openPnl[['Symbol', 'Target', 'stoploss']].to_string()}")

                        if row["CurrentPrice"] <= row["Target"]:
                            self.openPnl.at[index, "stoploss"] = (df_PE.loc[:lastIndexTimeData[1]-60, 'c'].min())*2
                            self.openPnl.at[index, "Target"] = row["CurrentPrice"]
                            self.strategyLogger.info(f"{self.humanTime} TARGET HIT PE and stoploss shifted to: {self.openPnl.at[index, 'stoploss']}")
                            self.strategyLogger.info(f"Target: {self.openPnl.at[index, 'Target']}")
                            self.strategyLogger.info(f"{self.openPnl[['Symbol', 'Target', 'stoploss']].to_string()}")
                            PE_Target = True
                            

                        elif row["CurrentPrice"] >= row["stoploss"]:
                            exitType = "PE_Stoploss Hit"
                            self.exitOrder(index, exitType)


                        elif (lastIndexTimeData[1] in df_PE.index):
                            if df_PE.at[lastIndexTimeData[1], "HRSO"] > PE_High:
                                exitType = "PE_high_Break"
                                self.exitOrder(index, exitType)



            tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            callCounter= tradecount.get('CE',0)
            putCounter= tradecount.get('PE',0)

            # if self.humanTime.time() > time(15, 20):
            #     if CE_Target == False:
            #         CE_Ls=1

            #     if PE_Target == False:
            #         PE_Ls=1
                    
                    


            # Check for entry signals and execute orders
            if ((timeData-60) in df.index) and self.humanTime.time() < time(15, 20) and self.humanTime.time() > time(9, 16) and MaxLoss_Hit == False:
                
                if df_CE is not None:
                    if (lastIndexTimeData[1] in df_CE.index) and callCounter < 1:
                        if df_CE.at[lastIndexTimeData[1], "HRSO"] < CE_Low and CE_Target == False:

                            CE_entry_price = df_CE.at[lastIndexTimeData[1], "c"]

                            if 100<=CE_entry_price<=300:
                                target = 0.2 * CE_entry_price
                                stoploss = 1.5 * CE_entry_price

                                self.entryOrder(CE_entry_price, callSym, lotSize*CE_Ls, "SELL", {"Expiry": expiryEpoch,"Target": target,"stoploss":stoploss},)
                            else:
                                New_Call_Entry = True

                if df_PE is not None:
                    if (lastIndexTimeData[1] in df_PE.index) and putCounter < 1:
                        if df_PE.at[lastIndexTimeData[1], "HRSO"] < PE_Low and PE_Target == False:
                        
                            PE_entry_price = df_PE.at[lastIndexTimeData[1], "c"]
                            if 100<=PE_entry_price<=300:
                                target = 0.2 * PE_entry_price
                                stoploss = 1.5 * PE_entry_price

                                self.entryOrder(PE_entry_price, putSym, lotSize*PE_Ls, "SELL", {"Expiry": expiryEpoch,"Target": target,"stoploss":stoploss},)
                            
                            else:
                                New_Put_Entry = True


                if New_Call_Entry:
                    callSym, entry_price = self.findValidOptionSymbol(
                        "CALL", df.at[lastIndexTimeData[1], "c"], CE_entry_price, baseSym, lastIndexTimeData, Currentexpiry, -1
                    )
                    
                    if callSym and entry_price:
                        target = 0.2 * entry_price
                        stoploss = 1.5 * entry_price
                        self.entryOrder(entry_price, callSym, lotSize*CE_Ls, "SELL", 
                                    {"Expiry": expiryEpoch, "Target": target, "stoploss": stoploss})
                        
                        New_Call_Entry = False
                        df_CE = None  # Reset CE DataFrame to fetch new symbol data in next iteration

                if New_Put_Entry:
                    putSym, entry_price = self.findValidOptionSymbol(
                        "PUT", df.at[lastIndexTimeData[1], "c"], PE_entry_price, baseSym, lastIndexTimeData, Currentexpiry, -1
                    )
                    
                    if putSym and entry_price:
                        target = 0.2 * entry_price
                        stoploss = 1.5 * entry_price
                        self.entryOrder(entry_price, putSym, lotSize*PE_Ls, "SELL", 
                                    {"Expiry": expiryEpoch, "Target": target, "stoploss": stoploss})
                    
                        New_Put_Entry = False
                        df_PE = None  # Reset PE DataFrame to fetch new symbol data in next iteration



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