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


class algoLogic(optOverNightAlgoLogic):

    def getOTMFactor(self, baseSym, Currentexpiry, lastIndexTimeData, Perc, df):
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

    def squareoff_build(self, build_num):
        for index, row in self.openPnl.iterrows():
            if row.get("Build") == build_num:
                self.exitOrder(index, f"StraddleExit_B{build_num}")

    def OptChain(self, date, symbol, IndexPrice, baseSym, Strangle_data, otm):
        prev_premium = None
        prev_symbol = None
        nearest_premium = None
        Sym = None

        if symbol == "CE":
            for i in range(0, otm + 1):
                callSymotm = self.getCallSym(date, baseSym, IndexPrice, otmFactor=i)
                try:
                    data = self.fetchAndCacheFnoHistData(callSymotm, date)
                    current_premium = data["c"]
                    if current_premium == Strangle_data:
                        nearest_premium = current_premium
                        Sym = callSymotm
                        break
                    if current_premium < Strangle_data:
                        if prev_premium is None:
                            nearest_premium = current_premium
                            Sym = callSymotm
                        else:
                            if abs(prev_premium - Strangle_data) <= abs(current_premium - Strangle_data):
                                nearest_premium = prev_premium
                                Sym = prev_symbol
                            else:
                                nearest_premium = current_premium
                                Sym = callSymotm
                        break
                    prev_premium = current_premium
                    prev_symbol = callSymotm
                except Exception as e:
                    self.strategyLogger.info(e)
            if Sym is None:
                self.strategyLogger.info(f"No premium found below target {Strangle_data}. Selecting closest above.")
                nearest_premium = prev_premium
                Sym = prev_symbol

        if symbol == "PE":
            for i in range(0, otm + 1):
                putSymotm = self.getPutSym(date, baseSym, IndexPrice, otmFactor=i)
                try:
                    data = self.fetchAndCacheFnoHistData(putSymotm, date)
                    current_premium = data["c"]
                    if current_premium == Strangle_data:
                        nearest_premium = current_premium
                        Sym = putSymotm
                        break
                    if current_premium < Strangle_data:
                        if prev_premium is None:
                            nearest_premium = current_premium
                            Sym = putSymotm
                        else:
                            if abs(prev_premium - Strangle_data) <= abs(current_premium - Strangle_data):
                                nearest_premium = prev_premium
                                Sym = prev_symbol
                            else:
                                nearest_premium = current_premium
                                Sym = putSymotm
                        break
                    prev_premium = current_premium
                    prev_symbol = putSymotm
                except Exception as e:
                    self.strategyLogger.info(e)
            if Sym is None:
                self.strategyLogger.info(f"No premium found below target {Strangle_data}. Selecting closest above.")
                nearest_premium = prev_premium
                Sym = prev_symbol

        self.strategyLogger.info(f"Selected premium: {nearest_premium} Strike: {Sym}")
        return Sym, nearest_premium

    def _do_strangle_entry(self, baseSym, df, lastIndexTimeData, Currentexpiry, otm, lotSize, expiryEpoch, build_num):
        callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry, otmFactor=otm)
        try:
            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
        except Exception as e:
            self.strategyLogger.info(e)
            return None, None, None, None, False
        data_CE = data["c"]

        putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry, otmFactor=otm)
        try:
            data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
        except Exception as e:
            self.strategyLogger.info(e)
            return None, None, None, None, False
        data_PE = data["c"]

        if data_CE > data_PE:
            if data_CE - data_PE > data_CE * 0.1:
                self.strategyLogger.info(f"B{build_num}: CE {data_CE} > PE {data_PE}. Rebalancing PE.")
                putSym, data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, data_CE, otm)
        elif data_PE > data_CE:
            if data_PE - data_CE > data_PE * 0.1:
                self.strategyLogger.info(f"B{build_num}: PE {data_PE} > CE {data_CE}. Rebalancing CE.")
                callSym, data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, data_PE, otm)
        else:
            self.strategyLogger.info(f"B{build_num}: CE & PE equal at {self.humanTime}.")

        return data_CE, data_PE, callSym, putSym, True

    def _handle_leg_adjustment(self, small_row, big_row, doubled_price, Half_price, symSide, build_num, baseSym, df, lastIndexTimeData, Currentexpiry, lotSize, expiryEpoch, StraddlePremium_Cr, refrence_value, Perc, EntryAllowed):
        small_idx = small_row.name
        big_idx = big_row.name

        # Guard: check both indices still exist in openPnl
        if small_idx not in self.openPnl.index or big_idx not in self.openPnl.index:
            self.strategyLogger.info(f"B{build_num}: Index {small_idx} or {big_idx} no longer in openPnl. Skipping adjustment.")
            return Perc, EntryAllowed

        if StraddlePremium_Cr is not None and refrence_value is not None and StraddlePremium_Cr < refrence_value:
            if symSide == "CE":
                callSym_ref = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry, otmFactor=1)
                otmstk = re.search(r'(\d+)(?=CE)', callSym_ref).group(1)
                callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                stk = re.search(r'(\d+)(?=CE)', callSym).group(1)

                if stk > otmstk:
                    if small_idx in self.openPnl.index:
                        self.exitOrder(small_idx, f"Half_Exit_B{build_num}")
                    callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                    if Data_CE < 1:
                        self.squareoff_build(build_num)
                        if Perc == 2:
                            Perc = 1
                        elif Perc == 1:
                            EntryAllowed = False
                            self.strategyLogger.info(f"B{build_num}: EntryAllowed -> False")
                    else:
                        self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": build_num})
                else:
                    if big_idx in self.openPnl.index:
                        self.exitOrder(big_idx, f"Half_Exit_B{build_num}")
                    putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, Half_price, otm=15)
                    if Data_PE < 1:
                        self.squareoff_build(build_num)
                        if Perc == 2:
                            Perc = 1
                        elif Perc == 1:
                            EntryAllowed = False
                            self.strategyLogger.info(f"B{build_num}: EntryAllowed -> False")
                    else:
                        self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": build_num})

            elif symSide == "PE":
                putSym_ref = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry, otmFactor=1)
                otmstk = re.search(r'(\d+)(?=PE)', putSym_ref).group(1)
                putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                stk = re.search(r'(\d+)(?=PE)', putSym).group(1)

                if stk < otmstk:
                    if small_idx in self.openPnl.index:
                        self.exitOrder(small_idx, f"Half_Exit_B{build_num}")
                    putSym, Data_PE = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym, doubled_price, otm=15)
                    if Data_PE < 1:
                        self.squareoff_build(build_num)
                        if Perc == 2:
                            Perc = 1
                        elif Perc == 1:
                            EntryAllowed = False
                            self.strategyLogger.info(f"B{build_num}: EntryAllowed -> False")
                    else:
                        self.entryOrder(Data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": build_num})
                else:
                    if big_idx in self.openPnl.index:
                        self.exitOrder(big_idx, f"Half_Exit_B{build_num}")
                    callSym, Data_CE = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym, Half_price, otm=15)
                    if Data_CE < 1:
                        self.squareoff_build(build_num)
                        if Perc == 2:
                            Perc = 1
                        elif Perc == 1:
                            EntryAllowed = False
                            self.strategyLogger.info(f"B{build_num}: EntryAllowed -> False")
                    else:
                        self.entryOrder(Data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": build_num})
        else:
            self.squareoff_build(build_num)

        return Perc, EntryAllowed

    def run(self, startDate, endDate, baseSym, indexSym):
        col = ["Target", "stoploss", "Expiry", "Trailing_Target", "Build"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df.dropna(inplace=True)
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")

        lastIndexTimeData = [0, 0]
        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        # Build 1 state (20% decay)
        Perc_B1 = 2
        n_B1 = 4
        EntryAllowed_B1 = False
        First_Entry_B1 = True

        # Build 2 state (40% decay)
        Perc_B2 = 2
        n_B2 = 4
        EntryAllowed_B2 = False
        First_Entry_B2 = True

        MaxLoss_Hit = False

        # Shared state
        straddle_data = []
        straddle_ema = []
        current_ema = None
        refrence_value_B1 = None
        refrence_value_B2 = None
        StraddlePremium_Cr = None
        max_straddle_premium = 0

        for timeData in df.index:
            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if self.humanTime.date() == datetime(2026, 3, 2).date():
                continue
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData - 60)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            # Update current prices
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)

            self.pnlCalculator()

            # Expiry rollover
            if self.humanTime.date() > expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch = expiryDatetime.timestamp()
                Perc_B1 = 2
                n_B1 = 4
                EntryAllowed_B1 = False
                First_Entry_B1 = True
                Perc_B2 = 2
                n_B2 = 4
                EntryAllowed_B2 = False
                First_Entry_B2 = True
                straddle_data = []
                straddle_ema = []
                current_ema = None
                refrence_value_B1 = None
                refrence_value_B2 = None
                StraddlePremium_Cr = None
                max_straddle_premium = 0
                MaxLoss_Hit = False


            if not self.openPnl.empty:
                open_sum = int(self.openPnl['Pnl'].sum())

                self.closedPnl['ExitTime'] = pd.to_datetime(self.closedPnl['ExitTime'])
                currentDayClosedPnl = self.closedPnl[self.closedPnl['ExitTime'].dt.date == self.humanTime.date()]
                close_sum = int(currentDayClosedPnl['Pnl'].sum())

                self.strategyLogger.info(f"{self.humanTime} pnl_sum:{open_sum + close_sum}")

                if (open_sum + close_sum) < -5000:
                    for index, row in self.openPnl.iterrows():
                        self.exitOrder(index, "MaxLoss")

                    MaxLoss_Hit = True

            # Filter by build
            filtered_B1 = pd.DataFrame()
            filtered_B2 = pd.DataFrame()
            if not self.openPnl.empty:
                filtered_B1 = self.openPnl[(self.openPnl['PositionStatus'] == -1) & (self.openPnl['Build'] == 1)]
                filtered_B2 = self.openPnl[(self.openPnl['PositionStatus'] == -1) & (self.openPnl['Build'] == 2)]

            # B1: 50% strangle decay -> relax n
            if not filtered_B1.empty:
                cur_B1 = filtered_B1['CurrentPrice'].sum()
                ent_B1 = filtered_B1['EntryPrice'].sum()
                self.strategyLogger.info(f"B1 Strangle curr={cur_B1} entry={ent_B1}")
                if cur_B1 <= ent_B1 * 0.5:
                    n_B1 = 2
                    self.strategyLogger.info(f"B1 strangle <=50%. n_B1={n_B1}")

            # B2: 50% strangle decay -> relax n
            if not filtered_B2.empty:
                cur_B2 = filtered_B2['CurrentPrice'].sum()
                ent_B2 = filtered_B2['EntryPrice'].sum()
                self.strategyLogger.info(f"B2 Strangle curr={cur_B2} entry={ent_B2}")
                if cur_B2 <= ent_B2 * 0.5:
                    n_B2 = 2
                    self.strategyLogger.info(f"B2 strangle <=50%. n_B2={n_B2}")

            # Straddle Premium Calculation (shared)
            if ((timeData - 60) in df.index) and self.humanTime.time() < time(15, 20) and self.humanTime.date() == expiryDatetime.date():
                callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry)
                putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], expiry=Currentexpiry)
                try:
                    data_CE = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    data_PE = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    StraddlePremium_Cr = data_CE["c"] + data_PE["c"]
                    self.strategyLogger.info(f"Straddle Premium at {self.humanTime} = {StraddlePremium_Cr}")
                    if StraddlePremium_Cr > max_straddle_premium:
                        max_straddle_premium = StraddlePremium_Cr
                        self.strategyLogger.info(f"New max premium: {max_straddle_premium}")
                except Exception as e:
                    self.strategyLogger.info(e)
                    self.strategyLogger.info(f"Error fetching straddle data at {self.humanTime}.")

            # Time-based exit (all positions)
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    if self.humanTime.time() >= time(15, 20):
                        self.exitOrder(index, "Time Up")

            # ====================================================
            # BUILD 1: Leg adjustment (1:n ratio)
            # ====================================================
            if not filtered_B1.empty and len(filtered_B1) == 2:
                r1 = filtered_B1.iloc[0]
                r2 = filtered_B1.iloc[1]
                p1 = r1["CurrentPrice"]
                p2 = r2["CurrentPrice"]

                if p1 * n_B1 <= p2:
                    self.strategyLogger.info(f"B1 ratio hit: {r1['Symbol']}({p1}) x{n_B1} <= {r2['Symbol']}({p2})")
                    symSide = r1["Symbol"][-2:]
                    n_B1 = 4
                    Perc_B1, EntryAllowed_B1 = self._handle_leg_adjustment(
                        r1, r2, p2, p1, symSide, 1,
                        baseSym, df, lastIndexTimeData, Currentexpiry, lotSize, expiryEpoch,
                        StraddlePremium_Cr, refrence_value_B1, Perc_B1, EntryAllowed_B1)

                elif p2 * n_B1 <= p1:
                    self.strategyLogger.info(f"B1 ratio hit: {r2['Symbol']}({p2}) x{n_B1} <= {r1['Symbol']}({p1})")
                    symSide = r2["Symbol"][-2:]
                    n_B1 = 4
                    Perc_B1, EntryAllowed_B1 = self._handle_leg_adjustment(
                        r2, r1, p1, p2, symSide, 1,
                        baseSym, df, lastIndexTimeData, Currentexpiry, lotSize, expiryEpoch,
                        StraddlePremium_Cr, refrence_value_B1, Perc_B1, EntryAllowed_B1)

            # ====================================================
            # BUILD 2: Leg adjustment (1:n ratio)
            # ====================================================
            if not filtered_B2.empty and len(filtered_B2) == 2:
                r1 = filtered_B2.iloc[0]
                r2 = filtered_B2.iloc[1]
                p1 = r1["CurrentPrice"]
                p2 = r2["CurrentPrice"]

                if p1 * n_B2 <= p2:
                    self.strategyLogger.info(f"B2 ratio hit: {r1['Symbol']}({p1}) x{n_B2} <= {r2['Symbol']}({p2})")
                    symSide = r1["Symbol"][-2:]
                    n_B2 = 4
                    Perc_B2, EntryAllowed_B2 = self._handle_leg_adjustment(
                        r1, r2, p2, p1, symSide, 2,
                        baseSym, df, lastIndexTimeData, Currentexpiry, lotSize, expiryEpoch,
                        StraddlePremium_Cr, refrence_value_B2, Perc_B2, EntryAllowed_B2)

                elif p2 * n_B2 <= p1:
                    self.strategyLogger.info(f"B2 ratio hit: {r2['Symbol']}({p2}) x{n_B2} <= {r1['Symbol']}({p1})")
                    symSide = r2["Symbol"][-2:]
                    n_B2 = 4
                    Perc_B2, EntryAllowed_B2 = self._handle_leg_adjustment(
                        r2, r1, p1, p2, symSide, 2,
                        baseSym, df, lastIndexTimeData, Currentexpiry, lotSize, expiryEpoch,
                        StraddlePremium_Cr, refrence_value_B2, Perc_B2, EntryAllowed_B2)

            # ====================================================
            # ENTRY SIGNALS
            # ====================================================
            if ((timeData - 60) in df.index) and MaxLoss_Hit == False:

                b1_sell_empty = True
                b2_sell_empty = True
                if not self.openPnl.empty:
                    b1_sell_empty = self.openPnl[(self.openPnl['PositionStatus'] == -1) & (self.openPnl['Build'] == 1)].empty
                    b2_sell_empty = self.openPnl[(self.openPnl['PositionStatus'] == -1) & (self.openPnl['Build'] == 2)].empty

                # ── BUILD 1 ENTRIES ──
                if b1_sell_empty and self.humanTime.date() == expiryDatetime.date() and self.humanTime.time() >= time(9, 20) and self.humanTime.time() < time(15, 20):
                    otm_b1 = self.getOTMFactor(baseSym, Currentexpiry, lastIndexTimeData, Perc_B1, df)

                    # B1: Re-entry after squareoff
                    if StraddlePremium_Cr is not None and refrence_value_B1 is not None and StraddlePremium_Cr < refrence_value_B1 and otm_b1 is not None and EntryAllowed_B1 == True:
                        data_CE, data_PE, callSym, putSym, success = self._do_strangle_entry(baseSym, df, lastIndexTimeData, Currentexpiry, otm_b1, lotSize, expiryEpoch, 1)
                        if success:
                            if data_CE < 1 or data_PE < 1:
                                self.strategyLogger.info(f"B1 re-entry: premium <1 (CE:{data_CE}, PE:{data_PE}). Skip.")
                                if Perc_B1 == 2:
                                    Perc_B1 = 1
                                elif Perc_B1 == 1:
                                    EntryAllowed_B1 = False
                                    self.strategyLogger.info(f"B1: EntryAllowed -> False")
                                continue
                            self.entryOrder(data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": 1})
                            self.entryOrder(data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": 1})
                            n_B1 = 4

                    # B1: First entry at 20% decay
                    if StraddlePremium_Cr is not None and max_straddle_premium > 0 and StraddlePremium_Cr <= max_straddle_premium * 0.9 and otm_b1 is not None and First_Entry_B1 == True:
                        data_CE, data_PE, callSym, putSym, success = self._do_strangle_entry(baseSym, df, lastIndexTimeData, Currentexpiry, otm_b1, lotSize, expiryEpoch, 1)
                        if success:
                            self.entryOrder(data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": 1})
                            self.entryOrder(data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": 1})
                            EntryAllowed_B1 = True
                            First_Entry_B1 = False
                            n_B1 = 4
                            refrence_value_B1 = StraddlePremium_Cr
                            self.strategyLogger.info(f"B1 reference value set: {refrence_value_B1}")

                # ── BUILD 2 ENTRIES ──
                if b2_sell_empty and self.humanTime.date() == expiryDatetime.date() and self.humanTime.time() >= time(9, 20) and self.humanTime.time() < time(15, 20):
                    otm_b2 = self.getOTMFactor(baseSym, Currentexpiry, lastIndexTimeData, Perc_B2, df)

                    # B2: Re-entry after squareoff
                    if StraddlePremium_Cr is not None and refrence_value_B2 is not None and StraddlePremium_Cr < refrence_value_B2 and otm_b2 is not None and EntryAllowed_B2 == True:
                        data_CE, data_PE, callSym, putSym, success = self._do_strangle_entry(baseSym, df, lastIndexTimeData, Currentexpiry, otm_b2, lotSize, expiryEpoch, 2)
                        if success:
                            if data_CE < 1 or data_PE < 1:
                                self.strategyLogger.info(f"B2 re-entry: premium <1 (CE:{data_CE}, PE:{data_PE}). Skip.")
                                if Perc_B2 == 2:
                                    Perc_B2 = 1
                                elif Perc_B2 == 1:
                                    EntryAllowed_B2 = False
                                    self.strategyLogger.info(f"B2: EntryAllowed -> False")
                                continue
                            self.entryOrder(data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": 2})
                            self.entryOrder(data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": 2})
                            n_B2 = 4


                    # B2: First entry at 40% decay
                    if StraddlePremium_Cr is not None and max_straddle_premium > 0 and StraddlePremium_Cr <= max_straddle_premium * 0.7 and otm_b2 is not None and First_Entry_B2 == True:
                        data_CE, data_PE, callSym, putSym, success = self._do_strangle_entry(baseSym, df, lastIndexTimeData, Currentexpiry, otm_b2, lotSize, expiryEpoch, 2)
                        if success:
                            self.entryOrder(data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": 2})
                            self.entryOrder(data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": 2})
                            EntryAllowed_B2 = True
                            First_Entry_B2 = False
                            n_B2 = 4
                            refrence_value_B2 = StraddlePremium_Cr
                            self.strategyLogger.info(f"B2 reference value set: {refrence_value_B2}")

        self.pnlCalculator()
        self.combinePnlCsv()
        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()
    devName = "NA"
    strategyName = "rdx"
    version = "v1"
    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2026, 12, 31, 15, 30)
    algo = algoLogic(devName, strategyName, version)
    baseSym = "SENSEX"
    indexName = "SENSEX"
    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)
    print("Calculating Daily Pnl")
    endTime = datetime.now()
    print(f"Done. Ended in {endTime - startTime}")