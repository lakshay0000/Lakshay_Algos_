import re
import numpy as np
import talib as ta
import pandas as pd
import pandas_ta as taa
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getFnoBacktestData

# sys.path.insert(1, '/root/backtestTools')


# Define a class algoLogic that inherits from optOverNightAlgoLogic
class algoLogic(optOverNightAlgoLogic):

    def getOTMFactor(self, baseSym, Currentexpiry, lastIndexTimeData, Perc, df):
        """
        Calculate OTM factor based on straddle premium.
        
        Args:
            baseSym: Base symbol (e.g., 'NIFTY', 'SENSEX')
            Currentexpiry: Current expiry date
            lastIndexTimeData: Last index time data [prev_time, current_time]
            Perc: Percentage multiplier for OTM calculation
            df: DataFrame with market data
        
        Returns:
            otm (int): Calculated OTM factor
            None: If exception occurs during data fetching
        """
        try:
            callSym = self.getCallSym(
                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry)
            putSym = self.getPutSym(
                self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry)
            
            data_CE = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
            data_PE = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
            
            StraddlePremium = data_CE["c"] + data_PE["c"]
            self.strategyLogger.info(f"Straddle Premium at {self.humanTime} is {StraddlePremium}")
            
            otm = round((StraddlePremium * Perc) / 50)
            self.strategyLogger.info(f"Calculated OTM factor is {otm} and Perc is {Perc}")
            
            return otm
            
        except Exception as e:
            self.strategyLogger.info(e)
            self.strategyLogger.info(f"Error fetching data at {self.humanTime}. Returning None.")
            return None

    def squareoff(self):
        for index, row in self.openPnl.iterrows():
            self.exitOrder(index, "StraddleExit")

    def OptChain(self, date, symbol, IndexPrice, baseSym, Strangle_data, otm):
        prev_premium = None
        prev_symbol = None
        nearest_premium = None
        Sym = None
        
        if (symbol == "CE"):
            for i in range(0, otm+1):
                callSymotm = self.getCallSym(date, baseSym, IndexPrice, otmFactor=i)         
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
                            # Compare which is closer
                            if abs(prev_premium - Strangle_data) <= abs(current_premium - Strangle_data):
                                nearest_premium = prev_premium
                                Sym = prev_symbol
                            else:
                                nearest_premium = current_premium
                                Sym = callSymotm
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
                putSymotm = self.getPutSym(date, baseSym, IndexPrice, otmFactor=i)
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
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        putentryallowed = False
        callentryallowed = False
        Perc = 2
        n = 4
        straddle_data = []  # List of dicts with timestamp and premium
        straddle_ema = []   # List of EMA values 
        current_ema = None
        refrence_value = None
        main_Trade = True
        EntryAllowed = False
        MaxLoss_Hit = False
        First_Entry = True
        StraddlePremium_Cr = None
        max_straddle_premium = 0


        

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # # Skip the dates 2nd March 2024 and 18th May 2024
            if self.humanTime.date() == datetime(2024, 4, 26).date():
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
                Perc = 2
                n = 4
                straddle_data = []  # List of dicts with timestamp and premium
                straddle_ema = []   # List of EMA values 
                current_ema = None
                refrence_value = None
                StraddlePremium_Cr = None
                max_straddle_premium = 0
                EntryAllowed = False
                First_Entry = True
            

            # if not self.openPnl.empty:
            #     open_sum = int(self.openPnl['Pnl'].sum())

            #     self.closedPnl['ExitTime'] = pd.to_datetime(self.closedPnl['ExitTime'])
            #     currentDayClosedPnl = self.closedPnl[self.closedPnl['ExitTime'].dt.date == self.humanTime.date()]
            #     close_sum = int(currentDayClosedPnl['Pnl'].sum())

            #     self.strategyLogger.info(f"{self.humanTime} pnl_sum:{open_sum + close_sum}")

            #     if (open_sum + close_sum) < -2000:
            #         for index, row in self.openPnl.iterrows():
            #             self.exitOrder(index, "MaxLoss")

            #         MaxLoss_Hit = True
            
            if not self.openPnl.empty:
                filtered = self.openPnl[self.openPnl['PositionStatus'] == -1]
                filtered_BUY = self.openPnl[self.openPnl['PositionStatus'] == 1]


            if not self.openPnl.empty and not filtered.empty:
                Current_strangle_value = self.openPnl[self.openPnl['PositionStatus'] == -1]['CurrentPrice'].sum()
                Entry_strangle_value = self.openPnl[self.openPnl['PositionStatus'] == -1]['EntryPrice'].sum()
                self.strategyLogger.info(f"Current Strangle Value: {Current_strangle_value}, Entry Strangle Value: {Entry_strangle_value}")

                if Current_strangle_value <= Entry_strangle_value * 0.5:
                    n = 2 
                    self.strategyLogger.info(f"Strangle value has reduced by 50%. Setting n to {n} for exit conditions.")



            if ((timeData-60) in df.index)and self.humanTime.time() >= time(12, 0) and self.humanTime.time() < time(15, 20) and self.humanTime.date() == expiryDatetime.date():
                # ... existing code to calculate callSym, putSym ...

                #Straddle Price Calculation
                callSym = self.getCallSym(
                    self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)
                putSym = self.getPutSym(
                    self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)
                
                try:
                    data_CE = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    data_PE = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    
                    StraddlePremium_Cr = data_CE["c"] + data_PE["c"]
                    self.strategyLogger.info(f"Straddle Premium at {self.humanTime} is {StraddlePremium_Cr}")

                    # Update maximum premium
                    if StraddlePremium_Cr > max_straddle_premium:
                        max_straddle_premium = StraddlePremium_Cr
                        self.strategyLogger.info(f"New highest premium: {max_straddle_premium}") 

                        
                except Exception as e:
                    self.strategyLogger.info(e)
                    self.strategyLogger.info(f"Error fetching data for {callSym} or {putSym} at {self.humanTime}. Skipping entry.")         



            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]  
                    

                    # Exit conditions for CE and PE legs
                    if self.humanTime.time() >= time(15, 20):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)  
                        

            if not self.openPnl.empty:
                if not filtered.empty:
                    if len(filtered) == 2:
                        # Get the two positions
                        row1 = filtered.iloc[0]
                        row2 = filtered.iloc[1]
                        
                        price1 = row1["CurrentPrice"]
                        price2 = row2["CurrentPrice"]

                        row1_index = row1.name
                        row2_index = row2.name
                        
                        # Check if one position's price is half or less than the other
                        if price1 * n <= price2:
                            # Exit position 1 (smaller price), keep position 2
                            self.strategyLogger.info(f"Position {row1['Symbol']} price ({price1}) is half of {row2['Symbol']} price ({price2}). Exiting {row1['Symbol']}.")
                            doubled_price = price2
                            Half_price = price1
                            symSide = row1["Symbol"]
                            symSide = symSide[len(symSide) - 2:]
                            self.strategyLogger.info(f"Doubled price for remaining position: {doubled_price} and symbol side is {symSide}")
                            n=4
                            
                            if StraddlePremium_Cr < refrence_value:
                                if symSide == "CE":
                                    callSym = self.getCallSym(
                                        self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor= 1)
                                    
                                    otmstk = re.search(r'(\d+)(?=CE)', callSym).group(1)

                                    callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                    
                                    stk = re.search(r'(\d+)(?=CE)', callSym).group(1)

                                    
                                    if stk > otmstk:
                                        self.exitOrder(row1_index, "Half_Exit")
                                        callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                        if Data_CE < 1:
                                            self.squareoff()
                                            if Perc == 2:
                                                Perc = 1
                                            elif Perc == 1:
                                                EntryAllowed = False
                                                self.strategyLogger.info(f"Setting EntryAllowed to False. No further entries will be taken.")
                                        else:
                                            self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                    else:
                                        self.exitOrder(row2_index, "Half_Exit")
                                        putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, Half_price , otm=15)
                                        if Data_PE < 1:
                                            self.squareoff()
                                            if Perc == 2:
                                                Perc = 1
                                            elif Perc == 1:
                                                EntryAllowed = False
                                                self.strategyLogger.info(f"Setting EntryAllowed to False. No further entries will be taken.")
                                        else:   
                                            self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)


                                elif symSide == "PE":
                                    putSym = self.getPutSym(
                                        self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor= 1)
                                    
                                    otmstk = re.search(r'(\d+)(?=PE)', putSym).group(1)

                                    putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                    stk = re.search(r'(\d+)(?=PE)', putSym).group(1)
                                    
                                    if stk < otmstk:
                                        self.exitOrder(row1_index, "Half_Exit")
                                        putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                        if Data_PE < 1:
                                            self.squareoff()
                                            if Perc == 2:
                                                Perc = 1
                                            elif Perc == 1:
                                                EntryAllowed = False
                                                self.strategyLogger.info(f"Setting EntryAllowed to False. No further entries will be taken.")
                                        else:
                                            self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                    else:
                                        self.exitOrder(row2_index, "Half_Exit")
                                        callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, Half_price , otm=15)
                                        if Data_CE < 1:
                                            self.squareoff()
                                            if Perc == 2:
                                                Perc = 1
                                            elif Perc == 1:
                                                EntryAllowed = False
                                                self.strategyLogger.info(f"Setting EntryAllowed to False. No further entries will be taken.")
                                        else:
                                            self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)


                            else:
                                self.squareoff()

                        
                        elif price2 * n <= price1:
                            # Exit position 2 (smaller price), keep position 1
                            self.strategyLogger.info(f"Position {row2['Symbol']} price ({price2}) is half of {row1['Symbol']} price ({price1}). Exiting {row2['Symbol']}.")
                            doubled_price = price1
                            Half_price = price2
                            self.strategyLogger.info(f"Doubled price for remaining position: {doubled_price}")
                            symSide = row2["Symbol"]
                            symSide = symSide[len(symSide) - 2:]
                            self.strategyLogger.info(f"Doubled price for remaining position: {doubled_price} and symbol side is {symSide}")
                            n=4

                            if StraddlePremium_Cr < refrence_value:
                                
                                if symSide == "CE":
                                    callSym = self.getCallSym(
                                            self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor= 1)

                                    otmstk = re.search(r'(\d+)(?=CE)', callSym).group(1)

                                    callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                    stk = re.search(r'(\d+)(?=CE)', callSym).group(1)

                                    
                                    if stk > otmstk:
                                        self.exitOrder(row2_index, "Half_Exit")
                                        callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                        if Data_CE < 1:
                                            self.squareoff()
                                            if Perc == 2:
                                                Perc = 1
                                            elif Perc == 1:
                                                EntryAllowed = False
                                                self.strategyLogger.info(f"Setting EntryAllowed to False. No further entries will be taken.")
                                        else:
                                            self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                    else:
                                        self.exitOrder(row1_index, "Half_Exit")
                                        putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, Half_price , otm=15)
                                        if Data_PE < 1:
                                            self.squareoff()
                                            if Perc == 2:
                                                Perc = 1
                                            elif Perc == 1:
                                                EntryAllowed = False
                                                self.strategyLogger.info(f"Setting EntryAllowed to False. No further entries will be taken.")
                                        else:
                                            self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)

                                elif symSide == "PE":
                                    putSym = self.getPutSym(
                                        self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor= 1)
                                    
                                    otmstk = re.search(r'(\d+)(?=PE)', putSym).group(1)

                                    putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                    stk = re.search(r'(\d+)(?=PE)', putSym).group(1)
                                    
                                    if stk < otmstk:
                                        self.exitOrder(row2_index, "Half_Exit")
                                        putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                        if Data_PE < 1:
                                            self.squareoff()
                                            if Perc == 2:
                                                Perc = 1
                                            elif Perc == 1:
                                                EntryAllowed = False
                                                self.strategyLogger.info(f"Setting EntryAllowed to False. No further entries will be taken.")
                                        else:
                                            self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                    else:
                                        self.exitOrder(row1_index, "Half_Exit")
                                        callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, Half_price , otm=15)
                                        if Data_CE < 1:
                                            self.squareoff()
                                            if Perc == 2:
                                                Perc = 1
                                            elif Perc == 1:
                                                EntryAllowed = False
                                                self.strategyLogger.info(f"Setting EntryAllowed to False. No further entries will be taken.")
                                        else:
                                            self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)

                                        
                            else:
                                self.squareoff()


            BuyPositionCounter= self.openPnl['PositionStatus'].value_counts().get(1,0)

            

            # Check for entry signals and execute orders
            if ((timeData-60) in df.index):

                if self.openPnl.empty and self.humanTime.date() == expiryDatetime.date() and self.humanTime.time() >= time(12, 0) and self.humanTime.time() < time(15, 20):
                    otm = self.getOTMFactor(baseSym, Currentexpiry, lastIndexTimeData, Perc, df)

                    if EntryAllowed == True and StraddlePremium_Cr < refrence_value and otm is not None:    
                        #Entry for CE and PE legs with OTM factor
                        callSym = self.getCallSym(
                            self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=otm)

                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        data_CE = data["c"]


                        putSym = self.getPutSym(
                            self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=otm)

                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        data_PE = data["c"]

                        if data_CE > data_PE:
                            if data_CE-data_PE > data_CE*0.1:
                                self.strategyLogger.info(f"CE premium {data_CE} is higher than PE premium {data_PE} at {self.humanTime}. Selecting strikes based on premium.")
                                putSym, data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, data_CE, otm)
                        elif data_PE > data_CE:
                            if data_PE-data_CE > data_PE*0.1:
                                self.strategyLogger.info(f"PE premium {data_PE} is higher than CE premium {data_CE} at {self.humanTime}. Selecting strikes based on premium.")
                                callSym, data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, data_PE, otm)
                        else:
                            self.strategyLogger.info(f"CE and PE premiums are equal at {self.humanTime}. Selecting strikes based on OTM factor.")

                        
                        if data_CE < 1 or data_PE < 1:
                            self.strategyLogger.info(f"One of the premiums is less than 5 (CE: {data_CE}, PE: {data_PE}). Skipping entry.")
                            if Perc == 2:
                                Perc = 1
                            elif Perc == 1:
                                EntryAllowed = False
                                self.strategyLogger.info(f"Setting EntryAllowed to False. No further entries will be taken.")
                            continue


                        self.entryOrder(data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                        self.entryOrder(data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)



                    if max_straddle_premium is not None and StraddlePremium_Cr is not None and StraddlePremium_Cr <= max_straddle_premium * 0.9 and otm is not None and First_Entry == True:
                        #Entry for CE and PE legs with OTM factor
                        refrence_value = StraddlePremium_Cr
                        self.strategyLogger.info(f"Straddle premium has reduced by 50% from the maximum premium. Setting reference value to current straddle premium: {refrence_value} for future comparisons.")

                        callSym = self.getCallSym(
                            self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=otm)

                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        data_CE = data["c"]


                        putSym = self.getPutSym(
                            self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=otm)

                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        data_PE = data["c"]

                        if data_CE > data_PE:
                            if data_CE-data_PE > data_CE*0.1:
                                self.strategyLogger.info(f"CE premium {data_CE} is higher than PE premium {data_PE} at {self.humanTime}. Selecting strikes based on premium.")
                                putSym, data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, data_CE, otm)
                        elif data_PE > data_CE:
                            if data_PE-data_CE > data_PE*0.1:
                                self.strategyLogger.info(f"PE premium {data_PE} is higher than CE premium {data_CE} at {self.humanTime}. Selecting strikes based on premium.")
                                callSym, data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, data_PE, otm)
                        else:
                            self.strategyLogger.info(f"CE and PE premiums are equal at {self.humanTime}. Selecting strikes based on OTM factor.")


                        if data_CE < 1 or data_PE < 1:
                            self.strategyLogger.info(f"One of the premiums is less than 5 (CE: {data_CE}, PE: {data_PE}). Skipping entry.")
                            if Perc == 2:
                                Perc = 1
                            elif Perc == 1:
                                EntryAllowed = False
                                self.strategyLogger.info(f"Setting EntryAllowed to False. No further entries will be taken.")
                            continue


                        self.entryOrder(data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                        self.entryOrder(data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                        EntryAllowed = True
                        First_Entry = False



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
    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2026, 12, 31, 15, 30)

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