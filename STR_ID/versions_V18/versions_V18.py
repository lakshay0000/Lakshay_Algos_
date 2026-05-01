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

            otm = round((StraddlePremium * Perc) / 100)
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
        n = 3
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
        exit_small_mode = False




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
                n = 3
                straddle_data = []  # List of dicts with timestamp and premium
                straddle_ema = []   # List of EMA values
                current_ema = None
                refrence_value = None
                StraddlePremium_Cr = None
                max_straddle_premium = 0
                EntryAllowed = False
                First_Entry = True
                exit_small_mode = False


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


            # if not self.openPnl.empty and not filtered.empty:
            #     Current_strangle_value = self.openPnl[self.openPnl['PositionStatus'] == -1]['CurrentPrice'].sum()
            #     Entry_strangle_value = self.openPnl[self.openPnl['PositionStatus'] == -1]['EntryPrice'].sum()
            #     self.strategyLogger.info(f"Current Strangle Value: {Current_strangle_value}, Entry Strangle Value: {Entry_strangle_value}")

            #     if Current_strangle_value <= Entry_strangle_value * 0.5:
            #         n = 2
            #         self.strategyLogger.info(f"Strangle value has reduced by 50%. Setting n to {n} for exit conditions.")



            if ((timeData-60) in df.index) and self.humanTime.time() < time(15, 20) and self.humanTime.date() == expiryDatetime.date():
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
                    if self.humanTime.time() >= time(15, 0):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)


            if not self.openPnl.empty:
                if not filtered.empty:
                    if len(filtered) == 2:
                        row1 = filtered.iloc[0]
                        row2 = filtered.iloc[1]

                        price1 = row1["CurrentPrice"]
                        price2 = row2["CurrentPrice"]

                        row1_index = row1.name
                        row2_index = row2.name

                        # row1 is smaller (price1), row2 is bigger (price2)
                        if price1 * n <= price2:
                            self.strategyLogger.info(f"1:4 triggered. {row1['Symbol']} ({price1}) vs {row2['Symbol']} ({price2}).")    
                            doubled_price = price2
                            Half_price = price1
                            symSide = row1["Symbol"]
                            symSide = symSide[len(symSide) - 2:]
                            otm = self.getOTMFactor(baseSym, Currentexpiry, lastIndexTimeData, Perc, df)

                            if otm is not None:

                                if exit_small_mode:
                                    # Continue exiting the smaller position (row1)
                                    self.strategyLogger.info(f"exit_small_mode active: exiting smaller {row1['Symbol']}.")     
                                    self.exitOrder(row1_index, "Half_Exit")
                                    if symSide == "CE":
                                        callSymRef = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry, otmFactor=0)
                                        otmstk = re.search(r'(\d+)(?=CE)', callSymRef).group(1)
                                        callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                        stk = re.search(r'(\d+)(?=CE)', callSym).group(1)

                                        if stk <= otmstk:
                                            exit_small_mode = False
                                            self.strategyLogger.info(f"New CE is at 1st OTM/ATM. Setting exit_small_mode to False")
                                            n = 3

                                        if Data_CE < 1:
                                            self.squareoff()
                                        else:
                                            self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)  

                                    elif symSide == "PE":
                                        putSymRef = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry, otmFactor=0)
                                        otmstk = re.search(r'(\d+)(?=PE)', putSymRef).group(1)
                                        putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                        stk = re.search(r'(\d+)(?=PE)', putSym).group(1)

                                        if stk >= otmstk:
                                            exit_small_mode = False
                                            self.strategyLogger.info(f"New PE is at 1st OTM/ATM. Setting exit_small_mode to False")
                                            n = 3

                                        if Data_PE < 1:
                                            self.squareoff()
                                        else:
                                            self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)

                                else:
                                    # Primary: exit bigger (row2), enter at Half_price on the bigger's side
                                    # symSide is the smaller's (row1) side; bigger's side is opposite
                                    if symSide == "CE":
                                        # smaller=CE, bigger=PE → primary: exit PE (row2), enter PE at Half_price
                                        putSym = self.getPutSym(
                                            self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry, otmFactor=otm)
                                        otmstk = re.search(r'(\d+)(?=PE)', putSym).group(1)

                                        putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, Half_price, otm=15)
                                        stk = re.search(r'(\d+)(?=PE)', putSym).group(1)

                                        if stk >= otmstk and Data_PE >= 1:
                                            # New PE is further OTM than 1st OTM → proceed with primary
                                            self.strategyLogger.info(f"Exiting bigger PE {row2['Symbol']}, entering PE at Half_price {Half_price}.")
                                            self.exitOrder(row2_index, "Half_Exit")
                                            self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                        else:
                                            # New PE is at 1st OTM/ATM or premium < 1 → fallback: exit smaller CE
                                            self.strategyLogger.info(f"PE at Half_price is at 1st OTM/ATM or premium<1. Fallback: exiting smaller CE {row1['Symbol']}.")
                                            self.exitOrder(row1_index, "Half_Exit")
                                            callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                            if Data_CE < 1:
                                                self.squareoff()
                                            else:
                                                self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                            exit_small_mode = True
                                            n = 4
                                            self.strategyLogger.info(f"Setting exit_small_mode to True.")

                                    elif symSide == "PE":
                                        # smaller=PE, bigger=CE → primary: exit CE (row2), enter CE at Half_price
                                        callSym = self.getCallSym(
                                            self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry, otmFactor=otm)
                                        otmstk = re.search(r'(\d+)(?=CE)', callSym).group(1)

                                        callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, Half_price, otm=15)
                                        stk = re.search(r'(\d+)(?=CE)', callSym).group(1)

                                        if stk <= otmstk and Data_CE >= 1:
                                            # New CE is further OTM than 1st OTM → proceed with primary
                                            self.strategyLogger.info(f"Exiting bigger CE {row2['Symbol']}, entering CE at Half_price {Half_price}.")
                                            self.exitOrder(row2_index, "Half_Exit")
                                            self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                        else:
                                            # New CE is at 1st OTM/ATM or premium < 1 → fallback: exit smaller PE
                                            self.strategyLogger.info(f"CE at Half_price is at 1st OTM/ATM or premium<1. Fallback: exiting smaller PE {row1['Symbol']}.")
                                            self.exitOrder(row1_index, "Half_Exit")
                                            self.strategyLogger.info(f"Attempting to enter CE at doubled price {doubled_price}.")
                                            putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                            if Data_PE < 1:
                                                self.squareoff()
                                            else:
                                                self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                            exit_small_mode = True
                                            n=4
                                            self.strategyLogger.info(f"Setting exit_small_mode to True.")

                        # row2 is smaller (price2), row1 is bigger (price1)
                        elif price2 * n <= price1:
                            self.strategyLogger.info(f"1:4 triggered. {row2['Symbol']} ({price2}) vs {row1['Symbol']} ({price1}).")
                            doubled_price = price1
                            Half_price = price2
                            symSide = row2["Symbol"]
                            symSide = symSide[len(symSide) - 2:]
                            otm = self.getOTMFactor(baseSym, Currentexpiry, lastIndexTimeData, Perc, df)

                            if otm is not None:

                                if exit_small_mode:
                                    # Continue exiting the smaller position (row2)
                                    self.strategyLogger.info(f"exit_small_mode active: exiting smaller {row2['Symbol']}.")
                                    self.exitOrder(row2_index, "Half_Exit")
                                    if symSide == "CE":
                                        callSymRef = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry, otmFactor=1)
                                        otmstk = re.search(r'(\d+)(?=CE)', callSymRef).group(1)
                                        callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                        stk = re.search(r'(\d+)(?=CE)', callSym).group(1)

                                        if stk >= otmstk:
                                            exit_small_mode = False
                                            self.strategyLogger.info(f"New CE is at 1st OTM/ATM. Setting exit_small_mode to False")
                                            n = 3

                                        if Data_CE < 1:
                                            self.squareoff()
                                        else:
                                            self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                            
                                    elif symSide == "PE":
                                        putSymRef = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry, otmFactor=1)
                                        otmstk = re.search(r'(\d+)(?=PE)', putSymRef).group(1)
                                        putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                        stk = re.search(r'(\d+)(?=PE)', putSym).group(1)

                                        if stk <= otmstk:
                                            exit_small_mode = False
                                            self.strategyLogger.info(f"New PE is at 1st OTM/ATM. Setting exit_small_mode to False")
                                            n = 3
                                            
                                        if Data_PE < 1:
                                            self.squareoff()
                                        else:
                                            self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)

                                else:
                                    # Primary: exit bigger (row1), enter at Half_price on the bigger's side
                                    if symSide == "CE":
                                        # smaller=CE, bigger=PE → primary: exit PE (row1), enter PE at Half_price
                                        putSym = self.getPutSym(
                                            self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry, otmFactor=otm)
                                        otmstk = re.search(r'(\d+)(?=PE)', putSym).group(1)

                                        putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, Half_price, otm=15)
                                        stk = re.search(r'(\d+)(?=PE)', putSym).group(1)

                                        if stk >= otmstk and Data_PE >= 1:
                                            self.strategyLogger.info(f"Exiting bigger PE {row1['Symbol']}, entering PE at Half_price {Half_price}.")
                                            self.exitOrder(row1_index, "Half_Exit")
                                            self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                        else:
                                            self.strategyLogger.info(f"PE at Half_price is at 1st OTM/ATM or premium<1. Fallback: exiting smaller CE {row2['Symbol']}.")
                                            self.exitOrder(row2_index, "Half_Exit")
                                            callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                            if Data_CE < 1:
                                                self.squareoff()
                                            else:
                                                self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                            exit_small_mode = True
                                            n = 4
                                            self.strategyLogger.info(f"Setting exit_small_mode to True.")

                                    elif symSide == "PE":
                                        # smaller=PE, bigger=CE → primary: exit CE (row1), enter CE at Half_price
                                        callSym = self.getCallSym(
                                            self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry, otmFactor=otm)
                                        otmstk = re.search(r'(\d+)(?=CE)', callSym).group(1)

                                        callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, Half_price, otm=15)
                                        stk = re.search(r'(\d+)(?=CE)', callSym).group(1)

                                        if stk <= otmstk and Data_CE >= 1:
                                            self.strategyLogger.info(f"Exiting bigger CE {row1['Symbol']}, entering CE at Half_price {Half_price}.")
                                            self.exitOrder(row1_index, "Half_Exit")
                                            self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                        else:
                                            self.strategyLogger.info(f"CE at Half_price is at 1st OTM/ATM or premium<1. Fallback: exiting smaller PE {row2['Symbol']}.")
                                            self.exitOrder(row2_index, "Half_Exit")
                                            self.strategyLogger.info(f"Attempting to enter CE at doubled price {doubled_price}.")
                                            putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                                            if Data_PE < 1:
                                                self.squareoff()
                                            else:
                                                self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                                
                                            exit_small_mode = True        
                                            n = 4  
                                            self.strategyLogger.info(f"Setting exit_small_mode to True.")


            BuyPositionCounter= self.openPnl['PositionStatus'].value_counts().get(1,0)



            # Check for entry signals and execute orders
            if ((timeData-60) in df.index):

                if self.openPnl.empty and self.humanTime.date() == expiryDatetime.date() and self.humanTime.time() >= time(9, 20) and self.humanTime.time() < time(15, 20):

                    if StraddlePremium_Cr <= max_straddle_premium * 0.9 and First_Entry == True:
                        #Entry for CE and PE legs with OTM factor
                        self.strategyLogger.info(f"Straddle premium has reduced by 50% from the maximum premium. Setting reference value to current straddle premium: {refrence_value} for future comparisons.")

                        callSym = self.getCallSym(
                            self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)

                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        data_CE = data["c"]


                        putSym = self.getPutSym(
                            self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry)

                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        data_PE = data["c"]


                        self.entryOrder(data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                        self.entryOrder(data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
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
    baseSym = "SENSEX"
    indexName = "SENSEX"

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
