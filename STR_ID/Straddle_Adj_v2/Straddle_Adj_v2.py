import numpy as np
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getFnoBacktestData

# sys.path.insert(1, '/root/backtestTools')


# Define a class algoLogic that inherits from optOverNightAlgoLogic
class algoLogic(optOverNightAlgoLogic):


    def _strike_with_floor(self, sym_getter, baseSym, spot, ts, premium_floor, expiry=None, max_steps=20):
        """Pick a strike whose premium > premium_floor.

        Starts at otmFactor=0 and walks ITM (-1, -2, ...) until the floor is met.
        Returns (sym, data); data is None if the option chain fetch failed.
        """
        sym = None
        data = None
        for step in range(max_steps):
            otm = -step
            try:
                if expiry is None:
                    sym = sym_getter(self.timeData, baseSym, spot, otmFactor=otm)
                else:
                    sym = sym_getter(self.timeData, baseSym, spot, expiry=expiry, otmFactor=otm)
                data = self.fetchAndCacheFnoHistData(sym, ts)
            except Exception as e:
                self.strategyLogger.info(f"Strike fetch failed {sym}: otm={otm}: {e}")
                return sym, None
            if data["c"] > premium_floor:
                return sym, data
        return sym, data


    def straddle(self, date, IndexPrice, baseSym):
        Factor = [0, 1, -1, 2, -2]
        minlist = []
        callSymList = []
        putSymList = []

        for i in Factor:
            callSym = self.getCallSym(self.timeData, baseSym, IndexPrice, otmFactor=i)
            try:
                dataCE = self.fetchAndCacheFnoHistData(callSym, date)
            except Exception as e:
                self.strategyLogger.info(e)
                continue  # Skip this iteration if call data can't be fetched

            if i != 0:
                i_put = -i  # Ensure otmFactor is positive for put options
            else:
                i_put = 0

            putSym = self.getPutSym(self.timeData, baseSym, IndexPrice, otmFactor=i_put)
            try:
                dataPE = self.fetchAndCacheFnoHistData(putSym, date)
            except Exception as e:
                self.strategyLogger.info(e)
                continue  # Skip this iteration if put data can't be fetched

            Diff = abs(dataCE["c"] - dataPE["c"])
            minlist.append(Diff)
            callSymList.append(callSym)
            putSymList.append(putSym)


        # Find the index of the minimum difference
        if minlist:  # Check if minlist is not empty
            self.strategyLogger.info(f"Straddle Option Pairs: {callSymList}")
            self.strategyLogger.info(f"Straddle Option Pairs: {putSymList}")
            self.strategyLogger.info(f"Straddle Option Premium Differences: {minlist}")   

            minIndex = np.argmin(minlist)
            return callSymList[minIndex], putSymList[minIndex]
        else:
            return None  # Return None if no valid pairs found


    # Define a method to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):

        # Add necessary columns to the DataFrame
        col = ["Target", "stoploss", "Expiry", "Trailing_Target"]
        self.addColumnsToOpenPnlDf(col)

        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            # Fetch historical data for backtesting
            df = getFnoBacktestData(indexSym, startEpoch-(86400*50), endEpoch, "1Min")
        except Exception as e:
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df.dropna(inplace=True)

        df = df[df.index >= startEpoch]

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")


        lastIndexTimeData = [0, 0]

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        # Per-day state for the straddle-adjustment strategy
        last_reset_date = None
        straddle_entered = False
        pe_refs = None             # stack of historical PE ref points; pe_refs[-1] is the current ref
        ce_refs = None             # stack of historical CE ref points; ce_refs[-1] is the current ref
        pe_initial = None          # 9:16 PE entry price (floor of the stack)
        ce_initial = None          # 9:16 CE entry price (floor of the stack)
        original_pe_index = None   # openPnl index of the 9:16 PE
        original_ce_index = None   # openPnl index of the 9:16 CE
        gain_factor = 1.2          # +20% trigger on non-expiry days, +30% on expiry day
        premium_floor = 50         # min premium when selling: 50 non-expiry, 30 expiry day


        # Loop through each timestamp in the DataFrame index
        for timeData in df.index:

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # Skip time periods outside trading hours
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)


            # Strategy Specific Trading Time
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
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

            # Calculate and update PnL
            self.pnlCalculator()


            if self.humanTime.date() > expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch = expiryDatetime.timestamp()


            # New day reset — fresh start at 9:16
            if self.humanTime.date() != last_reset_date:
                straddle_entered = False
                pe_refs = None
                ce_refs = None
                pe_initial = None
                ce_initial = None
                original_pe_index = None
                original_ce_index = None
                # Expiry-day overrides for adjustment entries: 30% gain trigger, premium floor 30
                # Other days: 20% gain trigger, premium floor 50
                is_expiry_day = (self.humanTime.date() == expiryDatetime.date())
                gain_factor = 1.3 if is_expiry_day else 1.2
                premium_floor = 30 if is_expiry_day else 50
                last_reset_date = self.humanTime.date()
                self.strategyLogger.info(
                    f"{self.humanTime} Day setup: expiry_day={is_expiry_day}, gain_factor={gain_factor}, premium_floor={premium_floor}")


            # End-of-day square-off at 15:20 — exit everything, no new entries today
            if self.humanTime.time() >= time(15, 20):
                if not self.openPnl.empty:
                    for index, row in self.openPnl.iterrows():
                        self.exitOrder(index, "Time Up")
                continue

            if (timeData - 60) not in df.index:
                continue


            # ----- 9:16 straddle entry -----
            if not straddle_entered and self.humanTime.time() >= time(9, 17):
                spot = df.at[lastIndexTimeData[1], "c"]

                callSym, putSym = self.straddle(lastIndexTimeData[1], df.at[lastIndexTimeData[1], "c"], baseSym)

                try:
                    pe_data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    self.entryOrder(pe_data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch})
                    original_pe_index = self.openPnl.index[-1]
                    pe_initial = pe_data["c"]
                    pe_refs = [pe_data["c"]]
                except Exception as e:
                    self.strategyLogger.info(e)


                try:
                    ce_data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    self.entryOrder(ce_data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch})
                    original_ce_index = self.openPnl.index[-1]
                    ce_initial = ce_data["c"]
                    ce_refs = [ce_data["c"]]
                except Exception as e:
                    self.strategyLogger.info(e)

                straddle_entered = True
                self.strategyLogger.info(
                    f"{self.humanTime} Straddle entered: PE_init={pe_initial} CE_init={ce_initial}")
                continue


            if not straddle_entered or self.openPnl.empty:
                continue


            # Current prices of the originals (None if exited via 3-position rule)
            original_pe_price = None
            if original_pe_index is not None and original_pe_index in self.openPnl.index:
                original_pe_price = self.openPnl.at[original_pe_index, "CurrentPrice"]
                self.strategyLogger.info(
                    f"{self.humanTime} Original PE price={original_pe_price}, pe_refs={pe_refs}")

            original_ce_price = None        
            if original_ce_index is not None and original_ce_index in self.openPnl.index:
                original_ce_price = self.openPnl.at[original_ce_index, "CurrentPrice"]
                self.strategyLogger.info(
                    f"{self.humanTime} Original CE price={original_ce_price}, ce_refs={ce_refs}")

            spot = df.at[lastIndexTimeData[1], "c"]


            # ----- PE-trigger: original PE +gain% from pe_refs[-1] → SELL another CE; back to previous ref → exit last CE -----
            if original_pe_price is not None and pe_refs:

                if original_pe_price >= pe_refs[-1] * gain_factor:
                    self.strategyLogger.info(f"th: {pe_refs[-1] * gain_factor}")
                    # Pick a CE strike with premium > floor; walk ITM if ATM is too cheap
                    callSym, new_ce_data = self._strike_with_floor(
                        self.getCallSym, baseSym, spot, lastIndexTimeData[1], premium_floor, expiry=Currentexpiry)
                    if new_ce_data is not None:
                        self.entryOrder(new_ce_data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch})
                        # New ref = actual current price (not the +gain% threshold)
                        pe_refs.append(original_pe_price)
                        self.strategyLogger.info(
                            f"{self.humanTime} PE +{int((gain_factor-1)*100)}% trigger: sold {callSym} @ {new_ce_data['c']}, pe_refs={pe_refs}")

                        # 3 CE limit → exit the earliest CE (FIFO)
                        ce_rows = self.openPnl[self.openPnl['Symbol'].str[-2:] == 'CE'].sort_index()
                        if len(ce_rows) >= 3:
                            earliest_ce_idx = ce_rows.index[0]
                            self.exitOrder(earliest_ce_idx, "3 CE Limit")
                            self.strategyLogger.info(
                                f"{self.humanTime} 3-CE limit hit: exited earliest CE idx={earliest_ce_idx}")
                            if earliest_ce_idx == original_ce_index:
                                original_ce_index = None

                elif len(pe_refs) > 1 and original_pe_price <= pe_refs[-2]:
                    # PE decayed to the previous ref — that level is the stoploss for the last-added CE (LIFO)
                    ce_rows = self.openPnl[self.openPnl['Symbol'].str[-2:] == 'CE'].sort_index()
                    last_added_idx = None
                    for idx in ce_rows.index[::-1]:
                        if idx != original_ce_index:
                            last_added_idx = idx
                            break
                    if last_added_idx is not None:
                        self.exitOrder(last_added_idx, "PE Decay SL")
                        pe_refs.pop()
                        self.strategyLogger.info(
                            f"{self.humanTime} PE decay SL: exited last CE idx={last_added_idx}, pe_refs={pe_refs}")


            # ----- CE-trigger: original CE +gain% from ce_refs[-1] → SELL another PE; back to previous ref → exit last PE -----
            if original_ce_price is not None and ce_refs:

                if original_ce_price >= ce_refs[-1] * gain_factor:
                    # Pick a PE strike with premium > floor; walk ITM if ATM is too cheap
                    putSym, new_pe_data = self._strike_with_floor(
                        self.getPutSym, baseSym, spot, lastIndexTimeData[1], premium_floor, expiry=Currentexpiry)
                    if new_pe_data is not None:
                        self.entryOrder(new_pe_data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch})
                        # New ref = actual current price (not the +gain% threshold)
                        ce_refs.append(original_ce_price)
                        self.strategyLogger.info(
                            f"{self.humanTime} CE +{int((gain_factor-1)*100)}% trigger: sold {putSym} @ {new_pe_data['c']}, ce_refs={ce_refs}")

                        # 3 PE limit → exit the earliest PE (FIFO)
                        pe_rows = self.openPnl[self.openPnl['Symbol'].str[-2:] == 'PE'].sort_index()
                        if len(pe_rows) >= 3:
                            earliest_pe_idx = pe_rows.index[0]
                            self.exitOrder(earliest_pe_idx, "3 PE Limit")
                            self.strategyLogger.info(
                                f"{self.humanTime} 3-PE limit hit: exited earliest PE idx={earliest_pe_idx}")
                            if earliest_pe_idx == original_pe_index:
                                original_pe_index = None

                elif len(ce_refs) > 1 and original_ce_price <= ce_refs[-2]:
                    # CE decayed to the previous ref — that level is the stoploss for the last-added PE (LIFO)
                    pe_rows = self.openPnl[self.openPnl['Symbol'].str[-2:] == 'PE'].sort_index()
                    last_added_idx = None
                    for idx in pe_rows.index[::-1]:
                        if idx != original_pe_index:
                            last_added_idx = idx
                            break
                    if last_added_idx is not None:
                        self.exitOrder(last_added_idx, "CE Decay SL")
                        ce_refs.pop()
                        self.strategyLogger.info(
                            f"{self.humanTime} CE decay SL: exited last PE idx={last_added_idx}, ce_refs={ce_refs}")



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
    startDate = datetime(2026, 2, 24, 9, 15)
    endDate = datetime(2026, 2, 24, 15, 30)

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
