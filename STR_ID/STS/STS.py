import re
import numpy as np
import pandas as pd
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
        strangle_ref_value = None




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
                straddle_data = []  # List of dicts with timestamp and premium
                straddle_ema = []   # List of EMA values
                current_ema = None
                refrence_value = None
                StraddlePremium_Cr = None
                max_straddle_premium = 0
                EntryAllowed = False
                First_Entry = True
                strangle_ref_value = None


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

                        current_strangle = price1 + price2

                        # Set reference when strangle is newly formed after entry or adjustment
                        if strangle_ref_value is None:
                            strangle_ref_value = current_strangle
                            self.strategyLogger.info(f"Strangle reference value set to {strangle_ref_value:.2f}.")

                        # Trigger on 30% decay/gain
                        threshold = 0.30
                        gain_threshold = 0.10
                        decay_triggered = current_strangle <= strangle_ref_value * (1 - threshold)
                        gain_triggered = current_strangle >= strangle_ref_value * (1 + gain_threshold)

                        self.strategyLogger.info(f"Strangle: {current_strangle:.2f}, Ref: {strangle_ref_value:.2f}, Decay: {decay_triggered}, Gain: {gain_triggered}")

                        if decay_triggered or gain_triggered:

                            # row1 is smaller (price1 <= price2)
                            if price1 <= price2:
                                self.strategyLogger.info(f"Strangle trigger. {row1['Symbol']} ({price1}) smaller, {row2['Symbol']} ({price2}) bigger.")
                                symSide = row1["Symbol"][-2:]
                                symSide2 = row2["Symbol"][-2:]

                                strangle_ref_value = None  # reset for next cycle

                                if decay_triggered:
                                    # 30% DECAY: exit smaller (row1), enter at bigger price (price2)
                                    self.strategyLogger.info(f"30% DECAY triggered: Exiting smaller {row1['Symbol']} ({price1}), entering at bigger price {price2}.")
                                    self.exitOrder(row1_index, "Half_Exit")

                                    if symSide == "CE":
                                        callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, price2, otm=15)
                                        if Data_CE < 1:
                                            self.squareoff()
                                        else:
                                            self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                    elif symSide == "PE":
                                        putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, price2, otm=15)
                                        if Data_PE < 1:
                                            self.squareoff()
                                        else:
                                            self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)

                                elif gain_triggered:
                                    # 30% GAIN: exit bigger (row2), enter at smaller price (price1)
                                    self.strategyLogger.info(f"30% GAIN triggered: Exiting bigger {row2['Symbol']} ({price2}), entering at smaller price {price1}.")
                                    self.exitOrder(row2_index, "Half_Exit")

                                    if symSide2 == "CE":
                                        callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, price1, otm=15)
                                        if Data_CE < 1:
                                            self.squareoff()
                                        else:
                                            self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                    elif symSide2 == "PE":
                                        putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, price1, otm=15)
                                        if Data_PE < 1:
                                            self.squareoff()
                                        else:
                                            self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)

                            # row2 is smaller (price2 < price1)
                            else:
                                self.strategyLogger.info(f"Strangle trigger. {row2['Symbol']} ({price2}) smaller, {row1['Symbol']} ({price1}) bigger.")
                                symSide = row1["Symbol"][-2:]
                                symSide2 = row2["Symbol"][-2:]

                                strangle_ref_value = None  # reset for next cycle

                                if decay_triggered:
                                    # 30% DECAY: exit smaller (row2), enter at bigger price (price1)
                                    self.strategyLogger.info(f"30% DECAY triggered: Exiting smaller {row2['Symbol']} ({price2}), entering at bigger price {price1}.")
                                    self.exitOrder(row2_index, "Half_Exit")

                                    if symSide2 == "CE":
                                        callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, price1, otm=15)
                                        if Data_CE < 1:
                                            self.squareoff()
                                        else:
                                            self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                    elif symSide2 == "PE":
                                        putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, price1, otm=15)
                                        if Data_PE < 1:
                                            self.squareoff()
                                        else:
                                            self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)

                                elif gain_triggered:
                                    # 30% GAIN: exit bigger (row1), enter at smaller price (price2)
                                    self.strategyLogger.info(f"30% GAIN triggered: Exiting bigger {row1['Symbol']} ({price1}), entering at smaller price {price2}.")
                                    self.exitOrder(row1_index, "Half_Exit")

                                    if symSide == "CE":
                                        callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, price2, otm=15)
                                        if Data_CE < 1:
                                            self.squareoff()
                                        else:
                                            self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                                    elif symSide == "PE":
                                        putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, price2, otm=15)
                                        if Data_PE < 1:
                                            self.squareoff()
                                        else:
                                            self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)


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
