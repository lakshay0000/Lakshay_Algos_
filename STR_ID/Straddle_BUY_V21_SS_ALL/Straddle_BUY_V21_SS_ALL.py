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


# ============================================================
# HARD-CODED: Number of builds to deploy
# Decay thresholds: Build 1 = 10%, Build 2 = 30%, Build 3 = 50%, ...
# Formula: decay_pct = 10 + (build_num - 1) * 20
# Multiplier: max_straddle * (1 - decay_pct/100)
# ============================================================
NUM_BUILDS = 3  # <-- Change this to 2, 3, 4, ... as needed


class algoLogic(optOverNightAlgoLogic):

    @staticmethod
    def _decay_multiplier(build_num):
        """Return the fraction of max straddle premium at which build_num enters.
        Build 1 -> 0.90  (10% decay)
        Build 2 -> 0.70  (30% decay)
        Build 3 -> 0.50  (50% decay)
        Build N -> 1 - (0.10 + (N-1)*0.20)
        """
        decay_pct = 10 + (build_num - 1) * 20
        return 1 - decay_pct / 100

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

    # ────────────────────────────────────────────────────────
    # Helper: initialise / reset per-build state dicts
    # ────────────────────────────────────────────────────────
    @staticmethod
    def _init_build_state():
        """Return fresh per-build state dictionaries for all NUM_BUILDS."""
        Perc = {}
        n = {}
        EntryAllowed = {}
        First_Entry = {}
        refrence_value = {}
        for b in range(1, NUM_BUILDS + 1):
            Perc[b] = 2
            n[b] = 4
            EntryAllowed[b] = False
            First_Entry[b] = True
            refrence_value[b] = None
        return Perc, n, EntryAllowed, First_Entry, refrence_value

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
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Min.csv")

        lastIndexTimeData = [0, 0]
        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        # ── Per-build state (dynamic) ──
        Perc, n, EntryAllowed, First_Entry, refrence_value = self._init_build_state()

        # Log decay thresholds for clarity
        for b in range(1, NUM_BUILDS + 1):
            mult = self._decay_multiplier(b)
            decay_pct = 10 + (b - 1) * 20
            self.strategyLogger.info(f"Build {b}: enters at {decay_pct}% decay (multiplier {mult:.2f})")

        # Shared state
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

            # ── Expiry rollover ──
            if self.humanTime.date() > expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch = expiryDatetime.timestamp()
                # Reset ALL build states
                Perc, n, EntryAllowed, First_Entry, refrence_value = self._init_build_state()
                StraddlePremium_Cr = None
                max_straddle_premium = 0

            # ── Filter positions per build ──
            filtered = {}
            for b in range(1, NUM_BUILDS + 1):
                if not self.openPnl.empty:
                    filtered[b] = self.openPnl[(self.openPnl['PositionStatus'] == -1) & (self.openPnl['Build'] == b)]
                else:
                    filtered[b] = pd.DataFrame()

            # ── 50% strangle decay -> relax n (per build) ──
            for b in range(1, NUM_BUILDS + 1):
                if not filtered[b].empty:
                    cur = filtered[b]['CurrentPrice'].sum()
                    ent = filtered[b]['EntryPrice'].sum()
                    self.strategyLogger.info(f"B{b} Strangle curr={cur} entry={ent}")
                    if cur <= ent * 0.5:
                        n[b] = 2
                        self.strategyLogger.info(f"B{b} strangle <=50%. n[{b}]={n[b]}")

            # ── Straddle Premium Calculation (shared) ──
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

            # ── Time-based exit (all positions) ──
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    if self.humanTime.time() >= time(15, 20):
                        self.exitOrder(index, "Time Up")

            # ====================================================
            # LEG ADJUSTMENT LOOP — all builds
            # ====================================================
            for b in range(1, NUM_BUILDS + 1):
                if not filtered[b].empty and len(filtered[b]) == 2:
                    r1 = filtered[b].iloc[0]
                    r2 = filtered[b].iloc[1]
                    p1 = r1["CurrentPrice"]
                    p2 = r2["CurrentPrice"]

                    if p1 * n[b] <= p2:
                        self.strategyLogger.info(f"B{b} ratio hit: {r1['Symbol']}({p1}) x{n[b]} <= {r2['Symbol']}({p2})")
                        symSide = r1["Symbol"][-2:]
                        n[b] = 4
                        Perc[b], EntryAllowed[b] = self._handle_leg_adjustment(
                            r1, r2, p2, p1, symSide, b,
                            baseSym, df, lastIndexTimeData, Currentexpiry, lotSize, expiryEpoch,
                            StraddlePremium_Cr, refrence_value[b], Perc[b], EntryAllowed[b])

                    elif p2 * n[b] <= p1:
                        self.strategyLogger.info(f"B{b} ratio hit: {r2['Symbol']}({p2}) x{n[b]} <= {r1['Symbol']}({p1})")
                        symSide = r2["Symbol"][-2:]
                        n[b] = 4
                        Perc[b], EntryAllowed[b] = self._handle_leg_adjustment(
                            r2, r1, p1, p2, symSide, b,
                            baseSym, df, lastIndexTimeData, Currentexpiry, lotSize, expiryEpoch,
                            StraddlePremium_Cr, refrence_value[b], Perc[b], EntryAllowed[b])

            # ====================================================
            # ENTRY SIGNALS LOOP — all builds
            # ====================================================
            if ((timeData - 60) in df.index):

                for b in range(1, NUM_BUILDS + 1):
                    # Check if this build already has open sell positions
                    b_sell_empty = True
                    if not self.openPnl.empty:
                        b_sell_empty = self.openPnl[(self.openPnl['PositionStatus'] == -1) & (self.openPnl['Build'] == b)].empty

                    if b_sell_empty and self.humanTime.date() == expiryDatetime.date() and self.humanTime.time() >= time(9, 20) and self.humanTime.time() < time(15, 20):
                        otm_b = self.getOTMFactor(baseSym, Currentexpiry, lastIndexTimeData, Perc[b], df)

                        # Re-entry after squareoff
                        if (StraddlePremium_Cr is not None
                                and refrence_value[b] is not None
                                and StraddlePremium_Cr < refrence_value[b]
                                and otm_b is not None
                                and EntryAllowed[b] == True):
                            data_CE, data_PE, callSym, putSym, success = self._do_strangle_entry(
                                baseSym, df, lastIndexTimeData, Currentexpiry, otm_b, lotSize, expiryEpoch, b)
                            if success:
                                if data_CE < 1 or data_PE < 1:
                                    self.strategyLogger.info(f"B{b} re-entry: premium <1 (CE:{data_CE}, PE:{data_PE}). Skip.")
                                    if Perc[b] == 2:
                                        Perc[b] = 1
                                    elif Perc[b] == 1:
                                        EntryAllowed[b] = False
                                        self.strategyLogger.info(f"B{b}: EntryAllowed -> False")
                                    continue
                                self.entryOrder(data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": b})
                                self.entryOrder(data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": b})
                                n[b] = 4

                        # First entry at the build's decay threshold
                        decay_mult = self._decay_multiplier(b)
                        if (StraddlePremium_Cr is not None
                                and max_straddle_premium > 0
                                and StraddlePremium_Cr <= max_straddle_premium * decay_mult
                                and otm_b is not None
                                and First_Entry[b] == True):
                            data_CE, data_PE, callSym, putSym, success = self._do_strangle_entry(
                                baseSym, df, lastIndexTimeData, Currentexpiry, otm_b, lotSize, expiryEpoch, b)
                            if success:
                                self.entryOrder(data_CE, callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": b})
                                self.entryOrder(data_PE, putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Build": b})
                                EntryAllowed[b] = True
                                First_Entry[b] = False
                                n[b] = 4
                                refrence_value[b] = StraddlePremium_Cr
                                self.strategyLogger.info(f"B{b} reference value set: {refrence_value[b]}")

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