import os
import talib
import pandas_ta as taa
import logging
import pandas as pd
from datetime import datetime, time,timedelta
from configparser import ConfigParser
from strategyTools.priceFinder import getSym
import json
import numpy as np
import threading

from strategyTools.dataLogger import algoLoggerSetup
from strategyTools.tools import OHLCDataFetch
from strategyTools.infra import getCurrentExpiry,getNextExpiry
from strategyTools import dataFetcher, reconnect, getclientData, priceFinder
from strategyTools.statusUpdater import infoMessage, positionUpdator
from strategyTools.infra import getBuyLimitPrice, getSellLimitPrice, postOrderToDbLIMIT

def setup_logger(name, log_file, level=logging.INFO):
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logging.basicConfig(level=level, filemode='a', force=True)
    return logger

class SimpleLogger:
    def __init__(self, log_dir, log_name="custom_log"):
        os.makedirs(log_dir, exist_ok=True)
        today_str = datetime.now().strftime("%Y-%m-%d")
        self.txt_path = os.path.join(log_dir, f"{log_name}_{today_str}.txt")
        self.json_path = os.path.join(log_dir, f"{log_name}_{today_str}.json")
        self._lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        state['_lock'] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = threading.Lock()

    def log(self, data):
        with self._lock:
            # Log to text file
            with open(self.txt_path, "a") as f_txt:
                if isinstance(data, dict):
                    line = " | ".join(f"{k}:{v}" for k, v in data.items())
                    f_txt.write(line + "\n")
                else:
                    f_txt.write(str(data) + "\n")
            # Log to JSON file (as a list of events)
            if isinstance(data, dict):
                if not os.path.exists(self.json_path):
                    with open(self.json_path, "w") as f_json:
                        json.dump([], f_json)
                with open(self.json_path, "r+") as f_json:
                    logs = json.load(f_json)
                    logs.append(data)
                    f_json.seek(0)
                    json.dump(logs, f_json)
                    f_json.truncate()

class RSIStrategy:
    """
    RSI-based Option Selling Strategy.
    Sells CE when RSI > 60, sells PE when RSI < 30.
    Exits on target or stoploss as per defined rules.
    """

    def __init__(self, baseSym):
        # --- Read config ---
        self.config = ConfigParser()
        self.config.read('config.ini')
        self.algoName = self.config.get('inputParameters', 'algoName', fallback='EMA_CONVERGENCE')
        self.baseSym = baseSym

        # --- Directory setup ---
        logFileFolder, jsonFileFolder = algoLoggerSetup(self.algoName)
        self.fileDir = {
            "baseJson": f"{jsonFileFolder}",
            "openPositions": f"{jsonFileFolder}/OpenPositions",
            "closedPositions": f"{jsonFileFolder}/ClosedPositions",
            "baseLog": f"{logFileFolder}",
            # Remove 'StrategyLog' from the log path
            "stockLogs": f"{logFileFolder}/{self.baseSym}",}
        
        # Create directories if they do not exist
        for path in self.fileDir.values():
            os.makedirs(path, exist_ok=True)

        # # --- Logger: one file per day ---
        # today_str = datetime.now().strftime("%Y-%m-%d")
        # log_file_path = f"{self.fileDir['strategyLogs']}/log_{today_str}.log"
        # self.logger = setup_logger(self.algoName, log_file_path)

        stockLogDir = f"{self.fileDir['stockLogs']}"
        os.makedirs(stockLogDir, exist_ok=True)
        today_str = datetime.now().strftime("%Y-%m-%d")
        log_file_path = f"{stockLogDir}/log_{today_str}.log"
        self.stockLogger = setup_logger(self.baseSym, log_file_path)
        self.stockLogger.propagate = False

        self.mylogger = SimpleLogger(
            log_dir=f"/root/liveAlgos/algoLogs/{self.algoName}/NIFTY",
            log_name="my_custom_log"
        )

        # --- Symbol and lot size info ---
        self.symMap = {"NIFTY": "NIFTY 50"}
        
        self.strikeDistMap = {
            "NIFTY": int(self.config.get('inputParameters', 'niftyStrikeDistance', fallback=50))
        }
        
        self.lotSizeMap = {
            "NIFTY": int(self.config.get('inputParameters', 'niftyLotSize', fallback=50))
        }
        
        

        # --- State ---
        self.idMap = {}
        self.symListConn = None
        self.openPnl = pd.DataFrame(columns=[
            "EntryTime", "Symbol", "EntryPrice", "CurrentPrice", "Quantity",
            "PositionStatus", "TradeType", "Pnl", "Expiry"])
        
        self.closedPnl = pd.DataFrame(columns=["Key", "ExitTime", "Symbol", "EntryPrice", "ExitPrice", "Quantity", "PositionStatus", "Pnl", "ExitType"])
        self.candle_1Min = {'last_candle_time': 0, 'df': None}
        self.candle_15Min = {'last_candle_time': 0, 'df': None}
        self.maxlist = []
        self.Midlist_low = []  # Added for low mid list
        self.Midlist = []
        self.list1 = []
        self.list1_low = []  # Added for low list1
        self.Closelist = []
        self.Closelist_low = []  # Added for low close list
        self.Twoswinghigh = None
        self.Twoswinglow = None  # Added for low swing
        self.Tradeindexprice = None
        self.list1_high = None
        self.list1_low_low = None  # Added for low list1
        self.load_lists()

        # --- Flags ---
        self.MidFlag = False
        self.MidFlag_low = False  # Added for low mid flag
        self.PutEntryAllow = False
        self.CallEntryAllow = False  # Added for call entry allow
        self.PutReEntryAllow = False
        self.CallReEntryAllow = False
        self.flag1 = False
        self.flag2 = False
        self.BufferEntryPut= False
        self.BufferEntryCall = False  # Added for call buffer entry
        self.BufferSignal = False  # Added for buffer signal
        self.load_flags()  # Load flags from file

        # Load open and closed positions
        self.load_open_positions()
        self.realizedPnl = 0
        self.unrealizedPnl = 0
        self.netPnl = 0


    def getCallSym(self, expiry, indexPrice, strikeDist):
        """Generate CE symbol for ATM strike."""
        atm = int(round(indexPrice / strikeDist) * strikeDist)
        return f"{self.baseSym}{expiry}{atm}CE"

    def getPutSym(self, expiry, indexPrice, strikeDist):
        """Generate PE symbol for ATM strike."""
        atm = int(round(indexPrice / strikeDist) * strikeDist)
        return f"{self.baseSym}{expiry}{atm}PE"

    def entryOrder(self, instrumentID, symbol, entryPrice, quantity, orderSide, extraCols=None, hedge=False):

        extraPercent = float(self.config.get('inputParameters', 'optExtraPercent', fallback=0.01))

        if orderSide == "BUY":
            limitPrice = getBuyLimitPrice(entryPrice, extraPercent)

        else:
            limitPrice = getSellLimitPrice(entryPrice, extraPercent)

        # Post the entry order to the database
        postOrderToDbLIMIT(
            exchangeSegment="NSEFO",
            algoName=self.algoName,
            isLive=self.config.getboolean('inputParameters', 'islive', fallback=True),
            exchangeInstrumentID=instrumentID,
            orderSide=orderSide,
            orderQuantity=quantity,
            limitPrice=limitPrice,
            upperPriceLimit=(float(self.config.get(
                'inputParameters', 'upperPriceLimitPercent', fallback=1.05)) * limitPrice) if orderSide == "BUY" else 0,
            lowerPriceLimit=0 if orderSide == "BUY" else (
                float(self.config.get('inputParameters', 'lowerPriceLimitPercent', fallback=0.95)) * limitPrice),
            timePeriod=int(self.config.get('inputParameters', 'timeLimitOrder', fallback=60)),
            extraPercent=extraPercent,
        )

                
        # Record the position in openPnl DataFrame
        newTrade = pd.DataFrame({
            "EntryTime": datetime.now(),
            "Symbol": symbol,
            "EntryPrice": entryPrice,
            "CurrentPrice": entryPrice,
            "Quantity": quantity,
            "PositionStatus": 1 if orderSide == "BUY" else -1,
            "Pnl": 0
        }, index=[0])

        if extraCols:
            for key in extraCols.keys():
                newTrade[key] = extraCols[key]

        self.openPnl = pd.concat([self.openPnl, newTrade], ignore_index=True)
        self.openPnl.reset_index(inplace=True, drop=True)

        # if hedge:
        #     self.logger.info(f"ENTRY: {orderSide} Hedge: {symbol} @ {entryPrice}")
        # else:
        #     self.logger.info(f"ENTRY: {orderSide} {symbol} @ {entryPrice}")

        self.save_open_positions()  # <--- Add this line

    def exitOrder(self, index, instrumentID, exitPrice, exitType):
        trade_to_close = self.openPnl.loc[index].copy()  # Use .loc to get a copy of the row
        extraPercent = float(self.config.get('inputParameters', 'optExtraPercent', fallback=0.01))

        if trade_to_close['PositionStatus'] == 1:
            limitPrice = getSellLimitPrice(exitPrice, extraPercent)
            orderSide = "SELL"

        else:
            limitPrice = getBuyLimitPrice(exitPrice, extraPercent)
            orderSide = "BUY"

        # Post the exit order to the database
        postOrderToDbLIMIT(
            exchangeSegment="NSEFO",
            algoName=self.algoName,
            isLive=self.config.getboolean('inputParameters', 'islive', fallback=True),
            exchangeInstrumentID=instrumentID,
            orderSide=orderSide,
            orderQuantity=int(trade_to_close['Quantity']),
            limitPrice=int(limitPrice),
            upperPriceLimit=0 if trade_to_close['PositionStatus'] == 1 else (
                float(self.config.get('inputParameters', 'upperPriceLimitPercent', fallback=1.05)) * limitPrice),
            lowerPriceLimit=(float(self.config.get('inputParameters', 'lowerPriceLimitPercent', fallback=0.95))
                             * limitPrice) if trade_to_close['PositionStatus'] == 1 else 0,
            timePeriod=int(self.config.get('inputParameters', 'timeLimitOrder', fallback=60)),
            extraPercent=extraPercent,
        )
        
        # Remove the trade from openPnl DataFrame
        self.openPnl.drop(index=index, inplace=True)

        # Create a new row for closedPnl DataFrame
        trade_to_close['Key'] = trade_to_close['EntryTime']
        trade_to_close['ExitTime'] = datetime.now()
        trade_to_close['ExitPrice'] = exitPrice
        trade_to_close['Pnl'] = (trade_to_close['ExitPrice'] -
                                 trade_to_close['EntryPrice']) * trade_to_close['Quantity'] * trade_to_close['PositionStatus']
        trade_to_close['ExitType'] = exitType

        for col in self.openPnl.columns:
            if col not in self.closedPnl.columns:
                del trade_to_close[col]

         # Append the closed trade to closedPnl DataFrame
        self.closedPnl = pd.concat([self.closedPnl, pd.DataFrame([trade_to_close])], ignore_index=True)
        self.closedPnl.reset_index(inplace=True, drop=True)
        
        # self.logger.info(
        #     f'Exit {exitType}: {trade_to_close["Symbol"]} @ {exitPrice}'
        # )
        self.save_open_positions()   # <--- Add this line
        self.save_closed_positions() # <--- Add this line


    def updateOpenPrices(self):
        """Update the current price for all open positions."""
        if not self.openPnl.empty:
            for idx, row in self.openPnl.iterrows():
                symbol = row['Symbol']
                instrumentID = self.idMap.get(symbol, None)
                if instrumentID is not None:
                    try:
                        price = dataFetcher([instrumentID])[instrumentID]
                        self.openPnl.at[idx, "CurrentPrice"] = price
                    except Exception:
                        self.logger.warning(f"Could not fetch price for {symbol}")
                else:
                    self.mylogger.log(f"Instrument ID not found for {symbol}")
                        
            self.save_open_positions()

    def pnlCalculator(self):
        '''Method to calculate PnL'''
        if not self.openPnl.empty:
            self.openPnl["Pnl"] = (self.openPnl["CurrentPrice"] - self.openPnl["EntryPrice"]) * self.openPnl["Quantity"] * self.openPnl["PositionStatus"]
            self.unrealizedPnl = self.openPnl["Pnl"].sum()
            self.save_open_positions()
        else:
            self.unrealizedPnl = 0
        if not self.closedPnl.empty:
            self.realizedPnl = self.closedPnl["Pnl"].sum()
        else:
            self.realizedPnl = 0
            
        self.netPnl = self.unrealizedPnl + self.realizedPnl
        # self.openPnl["EntryTime"] = pd.to_datetime(self.openPnl["EntryTime"])
        # self.closedPnl["Key"] = pd.to_datetime(self.closedPnl["Key"])
        # self.closedPnl["ExitTime"] = pd.to_datetime(self.closedPnl["ExitTime"])

    def rename_col(self, df):
        df["ti"] = df.index
        df["o"] = df["Open"]
        df["h"] = df["High"]
        df["l"] = df["Low"]
        df["c"] = df["Close"]
        df["v"] = df["Volume"]
        df["sym"] = df["Symbol"]
        df["date"] = pd.to_datetime(df.index + 19800, unit='s')

        del df["Open"]
        del df["High"]
        del df["Low"]
        del df["Close"]
        del df["Volume"]

    def getHedgeSym(self, symbolToHedge, symbolPrice):
        # does not support option buying strategy
        symSide = symbolToHedge[len(symbolToHedge) - 2:]

        # openPositions = self.openPnl[self.openPnl['Symbol'].str.contains(
        #     symSide)]
        # openPositions = openPositions[openPositions['PositionStatus'] == -1]

        # if openPositions.empty:
        #     return

        idx = next(i for i, char in enumerate(symbolToHedge) if char.isdigit())
        baseSymWithExpiry = symbolToHedge[:idx + 7]
        baseSym = symbolToHedge[:idx]

        hedgeSym = "NotFound"
        pricePerc = 0.05
        priceReq = round(pricePerc * symbolPrice, 2)

        while (hedgeSym == "NotFound") and (pricePerc <= 0.2):
            priceReq = round(pricePerc * symbolPrice, 2)
            hedgeSym = getSym(symSide, baseSymWithExpiry,
                              priceReq, getclientData())
            pricePerc += 0.01

        if (hedgeSym == "NotFound"):
            return None

        return hedgeSym

    def getHedgeIndex(self, index):
        # does not support option buying strategy
        openPnlCopy = self.openPnl.copy()

        posEntryTime = openPnlCopy.at[index, 'EntryTime']
        optSymbol = openPnlCopy.at[index, 'Symbol']
        symSide = optSymbol[len(optSymbol) - 2:]

        openPositions = openPnlCopy[openPnlCopy['Symbol'].str.contains(
            symSide)]
        buyOpenPositions = openPositions[openPositions['PositionStatus'] == 1]

        if buyOpenPositions.empty:
            return None
        
        # Ensure datetime
        buyOpenPositions = buyOpenPositions.copy()
        buyOpenPositions['EntryTime'] = pd.to_datetime(buyOpenPositions['EntryTime'])
        posEntryTime = pd.to_datetime(posEntryTime)

        time_diff = abs(
            buyOpenPositions['EntryTime'] - posEntryTime)
        return time_diff.idxmin()
    
    def updateOpenPositionsInfra(self):

        combinedOpenPnl = pd.DataFrame(columns=["EntryTime", "Symbol", "EntryPrice", "CurrentPrice", "Quantity", "PositionStatus", "Pnl", "Expiry"])
        combinedOpenPnl = pd.concat([combinedOpenPnl, self.openPnl], ignore_index=True)
        combinedOpenPnl['EntryTime'] = combinedOpenPnl['EntryTime'].astype(str)
        positionUpdator(combinedOpenPnl, 'Process_1', self.algoName)


    def run(self):
        """Main strategy loop."""
        from time import sleep

        # --- Get Expiry ---
        currentExpiry = getCurrentExpiry(self.baseSym)
        currentExpiryDatetime = datetime.strptime(currentExpiry, "%d%b%y").replace(hour=15, minute=20)
        
        if self.openPnl.empty and not self.BufferSignal:
            # --- Fetch initial buffer candle data (before loop) ---
            currentDatetime = datetime.now()
            self.candle_15Min['df'], candle_flag_15Min, self.candle_15Min['last_candle_time'] = OHLCDataFetch(
                self.symMap[self.baseSym],
                currentDatetime.timestamp(),
                self.candle_15Min['last_candle_time'],
                15, 50,  # Fetch 100 candles for buffer
                self.candle_15Min['df'],
                self.stockLogger
            )

            # --- Prepare and calculate indicators on buffer ---
            df_15Min = self.candle_15Min['df']
            if df_15Min is not None and not df_15Min.empty:
                self.rename_col(df_15Min)
                df_15Min.dropna(inplace=True)
                df_15Min = df_15Min[(df_15Min['date'].dt.time >= time(9, 15)) & (df_15Min['date'].dt.time < time(15, 30))].copy()
                df_15Min.dropna(inplace=True)
                # Calculate indicators
                df_15Min['EMA20'] = talib.EMA(df_15Min['c'], timeperiod=20)
                df_15Min['EMA50'] = talib.EMA(df_15Min['c'], timeperiod=50)
                df_15Min['EMA100'] = talib.EMA(df_15Min['c'], timeperiod=100)
                df_15Min['EMA200'] = talib.EMA(df_15Min['c'], timeperiod=200)
                results = taa.stochrsi(df_15Min["c"], length=14, rsi_length=14, k=3, d=3)
                df_15Min["%K"] = results["STOCHRSIk_14_14_3_3"]
                df_15Min["%D"] = results["STOCHRSId_14_14_3_3"]
                df_15Min.dropna(inplace=True)

            # --- Swing High Detection (Breakout) ---
            df_15Min['cross_80'] = (df_15Min["%K"].shift(1) < 80) & (df_15Min["%K"] > 80)
            df_15Min['cross_20'] = (df_15Min["%K"].shift(1) > 20) & (df_15Min["%K"] < 20)
            df_15Min['group_high'] = df_15Min['cross_80'].cumsum()

            first_group_high = df_15Min['group_high'].min()
            df_high = df_15Min[df_15Min['group_high'] > first_group_high].copy()
            df_high['rownum'] = np.arange(len(df_high))

            first_cross_20 = (
                df_high[df_high['cross_20']]
                .groupby('group_high')['rownum']
                .min()
                .rename('end_idx')
                .reset_index()
            )
            df_high = df_high.reset_index()
            df_high = df_high.merge(first_cross_20, on='group_high', how='left')
            df_high['in_segment_high'] = df_high['rownum'] <= df_high['end_idx']
            self.maxlist = (
                df_high[df_high['in_segment_high']]
                .groupby('group_high')['h']
                .max()
                .tolist()
            )

            # --- Swing Low Detection (Breakdown) ---
            df_15Min['cross_20_down'] = (df_15Min["%K"].shift(1) > 20) & (df_15Min["%K"] < 20)
            df_15Min['cross_80_up'] = (df_15Min["%K"].shift(1) < 80) & (df_15Min["%K"] > 80)
            df_15Min['group_low'] = df_15Min['cross_20_down'].cumsum()

            first_group_low = df_15Min['group_low'].min()
            df_low = df_15Min[df_15Min['group_low'] > first_group_low].copy()
            df_low['rownum'] = np.arange(len(df_low))

            first_cross_80_up = (
                df_low[df_low['cross_80_up']]
                .groupby('group_low')['rownum']
                .min()
                .rename('end_idx')
                .reset_index()
            )
            df_low = df_low.reset_index()
            df_low = df_low.merge(first_cross_80_up, on='group_low', how='left')
            df_low['in_segment_low'] = df_low['rownum'] <= df_low['end_idx']
            self.minlist = (
                df_low[df_low['in_segment_low']]
                .groupby('group_low')['l']
                .min()
                .tolist()
            )

            # --- EMA Convergence ---
            ema_cols = ['EMA20', 'EMA50', 'EMA100', 'EMA200']
            df_15Min['ema_max'] = df_15Min[ema_cols].max(axis=1)
            df_15Min['ema_min'] = df_15Min[ema_cols].min(axis=1)
            df_15Min['ema_converged'] = (df_15Min['ema_max'] - df_15Min['ema_min']) < 50

            # --- After calculating ema_converged, swing highs, and swing lows ---

            self.PutEntryAllow = False
            self.CallEntryAllow = False
            self.BufferEntryPut = False
            self.BufferEntryCall = False

            if df_15Min['ema_converged'].any():
                last_conv_idx = df_15Min[df_15Min['ema_converged']].index[-1]

                # --- Find swing highs before convergence ---
                swing_highs_df = df_high[df_high['in_segment_high']].groupby('group_high').agg({'h': 'max', 'rownum': 'max'})
                swing_highs_before_conv = swing_highs_df[swing_highs_df['rownum'] < last_conv_idx]['h'].tolist()
                last_two_highs_before_conv = swing_highs_before_conv[-2:] if len(swing_highs_before_conv) >= 2 else swing_highs_before_conv
                self.Twoswinghigh = max(last_two_highs_before_conv) if last_two_highs_before_conv else None

                # --- Find swing lows before convergence ---
                swing_lows_df = df_low[df_low['in_segment_low']].groupby('group_low').agg({'l':'min', 'rownum':'max'})
                swing_lows_before_conv = swing_lows_df[swing_lows_df['rownum'] < last_conv_idx]['l'].tolist()
                last_two_lows_before_conv = swing_lows_before_conv[-2:] if len(swing_lows_before_conv) >= 2 else swing_lows_before_conv
                self.Twoswinglow = min(last_two_lows_before_conv) if last_two_lows_before_conv else None

                closes_after_conv = df_15Min.loc[last_conv_idx+1:, 'c']

                # Find the first event: breakdown or breakout
                cross_below_idx = None
                cross_above_idx = None

                if self.Twoswinglow is not None:
                    below = closes_after_conv[closes_after_conv < self.Twoswinglow]
                    if not below.empty:
                        cross_below_idx = below.index[0]
                if self.Twoswinghigh is not None:
                    above = closes_after_conv[closes_after_conv > self.Twoswinghigh]
                    if not above.empty:
                        cross_above_idx = above.index[0]

                # Decide which event happened first (if any)
                if cross_below_idx is not None and (cross_above_idx is None or cross_below_idx < cross_above_idx):
                    # Low breakdown happened first
                    self.CallEntryAllow = False
                    self.BufferEntryCall = True
                    self.PutEntryAllow = False
                    self.BufferEntryPut = False
                    self.Twoswinghigh = None 
                    self.Twoswinglow = None

                    # --- Turn off BufferEntryCall if close crosses above EMA200 after breakdown ---
                    closes_after_cross = df_15Min.loc[cross_below_idx+1:, ['c', 'EMA200']]
                    cross_above_ema200 = closes_after_cross[closes_after_cross['c'] > closes_after_cross['EMA200']]
                    if not cross_above_ema200.empty:
                        self.BufferEntryCall = False

                elif cross_above_idx is not None and (cross_below_idx is None or cross_above_idx < cross_below_idx):
                    # High breakout happened first
                    self.PutEntryAllow = False
                    self.BufferEntryPut = True
                    self.CallEntryAllow = False
                    self.BufferEntryCall = False
                    self.Twoswinghigh = None 
                    self.Twoswinglow = None

                    # --- Turn off BufferEntryPut if close crosses below EMA200 after breakout ---
                    closes_after_cross = df_15Min.loc[cross_above_idx+1:, ['c', 'EMA200']]
                    cross_below_ema200 = closes_after_cross[closes_after_cross['c'] < closes_after_cross['EMA200']]
                    if not cross_below_ema200.empty:
                        self.BufferEntryPut = False

                else:
                    # Neither happened yet after convergence
                    self.PutEntryAllow = True
                    self.CallEntryAllow = True
                    self.BufferEntryPut = False
                    self.BufferEntryCall = False

            # --- Save initial state --- 
            self.BufferSignal=True      
            self.save_lists()
            self.save_flags()



        while True:
            currentDatetime = datetime.now()

            # --- Market hours check ---
            if (currentDatetime.time() < time(9, 17)) or (currentDatetime.time() > time(15, 29)):
                sleep(0.1)
                continue

            self.candle_1Min['df'], candle_flag_1Min, self.candle_1Min['last_candle_time'] = OHLCDataFetch(
                self.symMap[self.baseSym],
                currentDatetime.timestamp(),
                self.candle_1Min['last_candle_time'],
                1, 10, self.candle_1Min['df'], self.stockLogger)

            if (candle_flag_1Min):


                df_1Min = self.candle_1Min['df']
                # df_15Min = self.convert_to_ist(df_15Min)
                self.rename_col(df_1Min)

                df_1Min.dropna(inplace=True)
                df_1Min = df_1Min[(df_1Min['date'].dt.time >= time(9, 15)) &(df_1Min['date'].dt.time < time(15, 30))].copy()

                df_1Min.dropna(inplace=True) 

                # making csv 
                df_1Min.to_csv("df_1Min.csv")
            
            # Get the last row
                last_close_1Min = df_1Min.iloc[-1]['c']
                second_last_close_1Min = df_1Min.iloc[-2]['c']

            # --- Fetch 15Min Candle Data ---
            self.candle_15Min['df'], candle_flag_15Min, self.candle_15Min['last_candle_time'] = OHLCDataFetch(
                self.symMap[self.baseSym],
                currentDatetime.timestamp(),
                self.candle_15Min['last_candle_time'],
                15, 50, self.candle_15Min['df'], self.stockLogger)
             

            if (candle_flag_15Min):

                df_15Min = self.candle_15Min['df']
                # df_15Min = self.convert_to_ist(df_15Min)
                self.rename_col(df_15Min)

                df_15Min.dropna(inplace=True)
                df_15Min = df_15Min[(df_15Min['date'].dt.time >= time(9, 15)) &(df_15Min['date'].dt.time < time(15, 30))].copy()

                df_15Min.dropna(inplace=True) 


                # --- Calculate RSI(14) on 1-min close ---
                df_15Min['EMA20'] =  talib.EMA(df_15Min['c'], timeperiod=20)
                df_15Min['EMA50'] = talib.EMA(df_15Min['c'], timeperiod=50)
                df_15Min['EMA100'] =  talib.EMA(df_15Min['c'], timeperiod=100)
                df_15Min['EMA200'] =  talib.EMA(df_15Min['c'], timeperiod=200)

                results = taa.stochrsi(df_15Min["c"], length=14, rsi_length=14, k=3, d=3)
                df_15Min["%K"] = results["STOCHRSIk_14_14_3_3"]
                df_15Min["%D"] = results["STOCHRSId_14_14_3_3"]
                
                df_15Min.dropna(inplace=True)

                # making csv 
                df_15Min.to_csv("df_15Min.csv")

                # Get the last row
                last_row = df_15Min.iloc[-1]
                second_last_row = df_15Min.iloc[-2]
                last_K = last_row['%K']
                second_last_K = second_last_row['%K']
                last_ema200= last_row['EMA200']
                second_Last_ema200 = second_last_row['EMA200']

                # Get the max and min among the EMAs for the last row
                ema_values = [last_row['EMA20'], last_row['EMA50'], last_row['EMA100'], last_row['EMA200']]
                ema_max = max(ema_values)
                ema_min = min(ema_values)
                last_close = df_15Min.iloc[-1]['c']
                second_last_close= df_15Min.iloc[-2]['c']
                last_high = df_15Min.iloc[-1]['h']
                last_low  = df_15Min.iloc[-1]['l']
                # self.logger.info(f"[15Min] => Close: {last_close} | RSI: {last_rsi}")

                # --- Log all RSI values for the current 1-min candles ---
                # self.mylogger.log.log(
                #     f"{last_row['date']}, O:{last_row['o']}, H:{last_row['h']}, L:{last_row['l']}, C:{last_row['c']}, %K:{last_row['%K']}, EMA20:{last_row['EMA20']}, EMA50:{last_row['EMA50']}, EMA100:{last_row['EMA100']}, EMA200:{last_row['EMA200']}"
                # )

            ########## change expiry one day before expiry #########
            if currentDatetime.date() >= (currentExpiryDatetime).date():
                # --- Get Expiry ---
                currentExpiry = getNextExpiry(self.baseSym)
                currentExpiryDatetime = datetime.strptime(currentExpiry, "%d%b%y").replace(hour=15, minute=20)




            ######### Checking for swing high###########
            if (candle_flag_15Min):

                if last_K > 80 and second_last_K < 80 and not self.flag1:              
                    self.flag1 = True
                    self.Closelist = []
                    infoMessage(algoName=self.algoName, message="RSICross80")
                    self.mylogger.log("RSICross80")


                if self.flag1:
                    self.Closelist.append(last_high)
                    if last_K < 20 and second_last_K > 20:
                        self.flag1 = False
                        swinghigh = max(self.Closelist)
                        self.maxlist.append(swinghigh)
                        # infoMessage(algoName=self.algoName, message="RSICross20")
                        self.mylogger.log("RSICross20 - HighSwingComplete")
                        if self.MidFlag:
                            self.MidFlag = False
                            self.Midlist.clear()

                        self.MidFlag = True



                ######### checking for swing low ###########

                if last_K < 20 and second_last_K > 20 and not self.flag2:              
                    self.flag2 = True
                    self.Closelist_low = []
                    infoMessage(algoName=self.algoName, message="RSICross20")
                    self.mylogger.log("RSICross20")


                if self.flag2:
                    self.Closelist_low.append(last_low)
                    if last_K > 80 and second_last_K < 80:
                        self.flag2 = False
                        swinglow = min(self.Closelist_low)
                        self.minlist.append(swinglow)
                        # infoMessage(algoName=self.algoName, message="RSICross80")
                        self.mylogger.log("RSICross80 - LowSwingComplete")
                        if self.MidFlag_low:
                            self.MidFlag_low = False
                            self.Midlist_low.clear()

                        self.MidFlag_low = True
                        

            if (candle_flag_1Min):            
                if not self.openPnl.empty:
                    tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
                    callCounter= tradecount.get('CE',0)
                    putCounter= tradecount.get('PE',0)
                else:
                    callCounter = 0
                    putCounter = 0


            ############ checking 50 points convergence ###########
            if (candle_flag_15Min):
                if  len(self.maxlist)>=2 and len(self.minlist)>=2 and (ema_max - ema_min) <50:
                    # infoMessage(algoName=self.algoName, message="EMAConvergence - 50 points convergence")
                    self.mylogger.log("EMAConvergence - 50 points convergence(Entry Allow)")
                    last_two_max = self.maxlist[-2:]
                    last_two_min = self.minlist[-2:]

                    if self.Midlist:
                        Midhigh = max(self.Midlist)
                        last_two_max.append(Midhigh) 

                    if self.Midlist_low:
                        Midlow = min(self.Midlist_low)
                        last_two_min.append(Midlow) 
                                
                    # Find the maximum of the updated last_two_max list
                    self.Twoswinghigh = max(last_two_max)
                    self.Twoswinglow = min(last_two_min)

                    self.PutEntryAllow = True
                    self.CallEntryAllow = True
                    self.PutReEntryAllow = False
                    self.CallReEntryAllow = False
                    if putCounter==0:
                        self.list1.clear()
                    if callCounter==0:
                        self.list1_low.clear()


                if not self.openPnl.empty:
                    if putCounter > 0:
                        self.list1.append(last_high)
                    if callCounter > 0:
                        self.list1_low.append(last_low)


            if (candle_flag_1Min):
                # --- Generate ATM Option Symbols ---
                strikeDist = self.strikeDistMap[self.baseSym]
                lotSize = self.lotSizeMap[self.baseSym]
                atm = int(round(last_close_1Min / strikeDist) * strikeDist)
                ce_sym = self.getCallSym(currentExpiry, atm, strikeDist)
                pe_sym = self.getPutSym(currentExpiry, atm, strikeDist)


                # --- Fetch Option Prices ---
                data, self.idMap, self.symListConn = reconnect(self.idMap, [ce_sym, pe_sym])
                ce_price = data.get(self.idMap.get(ce_sym), None)
                pe_price = data.get(self.idMap.get(pe_sym), None)


            if (candle_flag_15Min):
                self.mylogger.log(
                    f"{last_row['date']}, O:{last_row['o']}, H:{last_row['h']}, L:{last_row['l']}, C:{last_row['c']}, %K:{last_row['%K']}, EMA20:{last_row['EMA20']}, EMA50:{last_row['EMA50']}, EMA100:{last_row['EMA100']}, EMA200:{last_row['EMA200']}, pe_sym:{pe_sym}, pe_price:{pe_price}"
                )


            # --- EXIT LOGIC ---
            if (candle_flag_1Min):
                if not self.openPnl.empty:
                    self.updateOpenPrices()

                    for idx, row in self.openPnl.iterrows():
                        if row['PositionStatus'] == 1:
                            continue
                        entry_price = row["EntryPrice"]
                        curr_price = row["CurrentPrice"]
                        expiry = row["Expiry"]
                        Index_price = row["IndexPrice"]
                        symbol = row["Symbol"]
                        symSide = symbol[len(symbol) - 2:]
                        instrumentID = self.idMap.get(symbol, None)
                        if instrumentID is None:
                            continue
                        
                        if symSide == "PE":
                        # Target exit
                            if curr_price < 0.3 * entry_price:
                                hedgeIndex = self.getHedgeIndex(idx)
                                self.exitOrder(idx, instrumentID, curr_price, "targethit")
                                infoMessage(algoName=self.algoName, message=f"TargetHit: EXIT {symbol} @ {curr_price}")
                                self.mylogger.log(f"TargetHit: EXIT {symbol} @ {curr_price}")

                                if hedgeIndex is not None:
                                    infoMessage(algoName=self.algoName, message=f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")
                                    self.mylogger.log(f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")

                                    self.exitOrder(
                                        hedgeIndex, self.idMap.get(self.openPnl.at[hedgeIndex, 'Symbol']), self.openPnl.at[hedgeIndex, 'CurrentPrice'], "HEDGE")

                                # TargetRollover Entry
                                hedgeSym = self.getHedgeSym(pe_sym, pe_price)
                                
                                if hedgeSym:
                                    hedgeData, self.idMap, self.symListConn = reconnect(
                                        self.idMap, [hedgeSym])
                                    hedgePrice = hedgeData[self.idMap[hedgeSym]]
                                    self.entryOrder(
                                        self.idMap[hedgeSym], hedgeSym,  hedgePrice, lotSize, "BUY", {"Expiry": currentExpiryDatetime}, hedge=True)
                                    infoMessage(algoName=self.algoName, message=f"HedgeEntry: BUY PE {hedgeSym} @ {hedgePrice }")
                                    self.mylogger.log(f"HedgeEntry: BUY PE {hedgeSym} @ {hedgePrice }")

                                self.entryOrder(self.idMap[pe_sym], pe_sym, pe_price , lotSize, "SELL", {"Expiry": currentExpiryDatetime, "IndexPrice": self.Tradeindexprice})                   
                                infoMessage(algoName=self.algoName, message=f"ReEntryRollover: SELL PE {pe_sym} @ {pe_price }")
                                self.mylogger.log(f"ReEntryRollover: SELL PE {pe_sym} @ {pe_price }")

                            # Stoploss Exit
                            elif (last_close_1Min <= (Index_price -50)):
                                hedgeIndex = self.getHedgeIndex(idx)
                                self.exitOrder(idx, instrumentID, curr_price, "stoplossHit")
                                infoMessage(algoName=self.algoName, message=f"StoplossHit: EXIT {symbol} @ {curr_price}")
                                self.mylogger.log(f"StoplossHit: EXIT {symbol} @ {curr_price}")

                                if hedgeIndex is not None:
                                    infoMessage(algoName=self.algoName, message=f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")
                                    self.mylogger.log(f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")

                                    self.exitOrder(
                                        hedgeIndex, self.idMap.get(self.openPnl.at[hedgeIndex, 'Symbol']), self.openPnl.at[hedgeIndex, 'CurrentPrice'], "HEDGE")

                                # ReEntry
                                self.list1_high = max(self.list1)
                                self.PutReEntryAllow = True
                                self.PutEntryAllow = False
                            
                            # elif (candle_flag_15Min):
                            #     if (last_close < last_ema200) and (second_last_close > second_Last_ema200):
                            #         hedgeIndex = self.getHedgeIndex(idx)
                            #         self.exitOrder(idx, instrumentID, curr_price, "EMA200Cross")
                            #         infoMessage(algoName=self.algoName, message=f"EMA200cross: EXIT {symbol} @ {curr_price}")
                            #         self.mylogger.log(f"EMA200cross: EXIT {symbol} @ {curr_price}")

                            #         if hedgeIndex is not None:
                            #             infoMessage(algoName=self.algoName, message=f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")
                            #             self.mylogger.log(f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")

                            #             self.exitOrder(
                            #                 hedgeIndex, self.idMap.get(self.openPnl.at[hedgeIndex, 'Symbol']), self.openPnl.at[hedgeIndex, 'CurrentPrice'], "HEDGE")

                            #         # ReEntry
                            #         self.list1_high = max(self.list1)
                            #         self.PutReEntryAllow = True
                            #         self.PutEntryAllow = False
                            
                            # expiry exit
                            elif (datetime.now() >= pd.to_datetime(expiry).replace(tzinfo=None)):
                                hedgeIndex = self.getHedgeIndex(idx)
                                self.exitOrder(idx, instrumentID, curr_price, "ExpryTimeUp")
                                infoMessage(algoName=self.algoName, message=f"ExpryTimeUp: EXIT {symbol} @ {curr_price}")
                                self.mylogger.log(f"ExpryTimeUp: EXIT {symbol} @ {curr_price}")

                                if hedgeIndex is not None:
                                    infoMessage(algoName=self.algoName, message=f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")
                                    self.mylogger.log(f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")

                                    self.exitOrder(
                                        hedgeIndex, self.idMap.get(self.openPnl.at[hedgeIndex, 'Symbol']), self.openPnl.at[hedgeIndex, 'CurrentPrice'], "HEDGE")
                
                                                
                                ##### Expiry Rollover Entry #####
                                hedgeSym = self.getHedgeSym(pe_sym, pe_price)
                                
                                if hedgeSym:
                                    hedgeData, self.idMap, self.symListConn = reconnect(
                                        self.idMap, [hedgeSym])
                                    hedgePrice = hedgeData[self.idMap[hedgeSym]]
                                    self.entryOrder(
                                        self.idMap[hedgeSym], hedgeSym,  hedgePrice, lotSize, "BUY", {"Expiry": currentExpiryDatetime}, hedge=True)
                                    infoMessage(algoName=self.algoName, message=f"HedgeEntry: BUY PE {hedgeSym} @ {hedgePrice }")
                                    self.mylogger.log(f"HedgeEntry: BUY PE {hedgeSym} @ {hedgePrice }")

                                self.entryOrder(self.idMap[pe_sym], pe_sym, pe_price , lotSize, "SELL", {"Expiry": currentExpiryDatetime, "IndexPrice": self.Tradeindexprice})
                                infoMessage(algoName=self.algoName, message=f"ExpiryRollover: SELL PE {pe_sym} @ {pe_price }")
                                self.mylogger.log(f"ExpiryRollover: SELL PE {pe_sym} @ {pe_price }")

                        elif symSide == "CE":
                            # Target exit
                            if curr_price < 0.3 * entry_price:
                                hedgeIndex = self.getHedgeIndex(idx)
                                self.exitOrder(idx, instrumentID, curr_price, "targethit")
                                infoMessage(algoName=self.algoName, message=f"TargetHit: EXIT {symbol} @ {curr_price}")
                                self.mylogger.log(f"TargetHit: EXIT {symbol} @ {curr_price}")

                                if hedgeIndex is not None:
                                    infoMessage(algoName=self.algoName, message=f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")
                                    self.mylogger.log(f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")

                                    self.exitOrder(
                                        hedgeIndex, self.idMap.get(self.openPnl.at[hedgeIndex, 'Symbol']), self.openPnl.at[hedgeIndex, 'CurrentPrice'], "HEDGE")

                                # TargetRollover Entry
                                hedgeSym = self.getHedgeSym(ce_sym, ce_price)
                                
                                if hedgeSym:
                                    hedgeData, self.idMap, self.symListConn = reconnect(
                                        self.idMap, [hedgeSym])
                                    hedgePrice = hedgeData[self.idMap[hedgeSym]]
                                    self.entryOrder(
                                        self.idMap[hedgeSym], hedgeSym,  hedgePrice, lotSize, "BUY", {"Expiry": currentExpiryDatetime}, hedge=True)
                                    infoMessage(algoName=self.algoName, message=f"HedgeEntry: BUY CE {hedgeSym} @ {hedgePrice }")
                                    self.mylogger.log(f"HedgeEntry: BUY CE {hedgeSym} @ {hedgePrice }")

                                self.entryOrder(self.idMap[ce_sym], ce_sym, ce_price , lotSize, "SELL", {"Expiry": currentExpiryDatetime, "IndexPrice": self.Tradeindexprice})                    
                                infoMessage(algoName=self.algoName, message=f"ReEntryRollover: SELL CE {ce_sym} @ {ce_price }")
                                self.mylogger.log(f"ReEntryRollover: SELL CE {ce_sym} @ {ce_price }")

                            # Stoploss Exit
                            elif (last_close_1Min >= (Index_price +50)):
                                hedgeIndex = self.getHedgeIndex(idx)
                                self.exitOrder(idx, instrumentID, curr_price, "stoplosshit")
                                infoMessage(algoName=self.algoName, message=f"StoplossHit: EXIT {symbol} @ {curr_price}")
                                self.mylogger.log(f"StoplossHit: EXIT {symbol} @ {curr_price}")

                                if hedgeIndex is not None:
                                    infoMessage(algoName=self.algoName, message=f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")
                                    self.mylogger.log(f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")

                                    self.exitOrder(
                                        hedgeIndex, self.idMap.get(self.openPnl.at[hedgeIndex, 'Symbol']), self.openPnl.at[hedgeIndex, 'CurrentPrice'], "HEDGE")

                                # ReEntry
                                self.list1_low_low = min(self.list1_low)
                                self.CallReEntryAllow = True
                                self.CallEntryAllow = False
                            
                            # elif (candle_flag_15Min):
                            #     if (last_close > last_ema200) and (second_last_close < second_Last_ema200):
                            #         hedgeIndex = self.getHedgeIndex(idx)
                            #         self.exitOrder(idx, instrumentID, curr_price, "EMA200Cross")
                            #         infoMessage(algoName=self.algoName, message=f"EMA200cross: EXIT {symbol} @ {curr_price}")
                            #         self.mylogger.log(f"EMA200cross: EXIT {symbol} @ {curr_price}")

                            #         if hedgeIndex is not None:
                            #             infoMessage(algoName=self.algoName, message=f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")
                            #             self.mylogger.log(f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")

                            #             self.exitOrder(
                            #                 hedgeIndex, self.idMap.get(self.openPnl.at[hedgeIndex, 'Symbol']), self.openPnl.at[hedgeIndex, 'CurrentPrice'], "HEDGE")
                            #         # ReEntry
                            #         self.list1_low_low = min(self.list1_low)
                            #         self.CallReEntryAllow = True
                            #         self.CallEntryAllow = False
                            
                            # expiry exit
                            elif (datetime.now() >= pd.to_datetime(expiry).replace(tzinfo=None)):
                                hedgeIndex = self.getHedgeIndex(idx)
                                self.exitOrder(idx, instrumentID, curr_price, "ExpryTimeUp")
                                infoMessage(algoName=self.algoName, message=f"ExpryTimeUp: EXIT {symbol} @ {curr_price}")
                                self.mylogger.log(f"ExpryTimeUp: EXIT {symbol} @ {curr_price}")

                                if hedgeIndex is not None:
                                    infoMessage(algoName=self.algoName, message=f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")
                                    self.mylogger.log(f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")

                                    self.exitOrder(
                                        hedgeIndex, self.idMap.get(self.openPnl.at[hedgeIndex, 'Symbol']), self.openPnl.at[hedgeIndex, 'CurrentPrice'], "HEDGE")
                                                
                                ##### Expiry Rollover Entry #####
                                hedgeSym = self.getHedgeSym(ce_sym, ce_price)
                                
                                if hedgeSym:
                                    hedgeData, self.idMap, self.symListConn = reconnect(
                                        self.idMap, [hedgeSym])
                                    hedgePrice = hedgeData[self.idMap[hedgeSym]]
                                    self.entryOrder(
                                        self.idMap[hedgeSym], hedgeSym,  hedgePrice, lotSize, "BUY", {"Expiry": currentExpiryDatetime}, hedge=True)
                                    infoMessage(algoName=self.algoName, message=f"HedgeEntry: BUY CE {hedgeSym} @ {hedgePrice }")
                                    self.mylogger.log(f"HedgeEntry: BUY CE {hedgeSym} @ {hedgePrice }")

                                self.entryOrder(self.idMap[ce_sym], ce_sym, ce_price , lotSize, "SELL", {"Expiry": currentExpiryDatetime, "IndexPrice": self.Tradeindexprice})
                                infoMessage(algoName=self.algoName, message=f"ExpiryRollover: SELL CE {ce_sym} @ {ce_price }")
                                self.mylogger.log(f"ExpiryRollover: SELL CE {ce_sym} @ {ce_price }")


                    sleep(0.1)
                
            # --- PnL Calculation ---
                self.pnlCalculator()


            # --- EXIT LOGIC for 15Min Candle ---
            if (candle_flag_15Min):
                if not self.openPnl.empty:
                    self.updateOpenPrices()

                    for idx, row in self.openPnl.iterrows():
                        if row['PositionStatus'] == 1:
                            continue
                        entry_price = row["EntryPrice"]
                        curr_price = row["CurrentPrice"]
                        expiry = row["Expiry"]
                        Index_price = row["IndexPrice"]
                        symbol = row["Symbol"]
                        symSide = symbol[len(symbol) - 2:]
                        instrumentID = self.idMap.get(symbol, None)
                        if instrumentID is None:
                            continue

                        if symSide == "PE":
                            if (last_close < last_ema200) and (second_last_close > second_Last_ema200):
                                    hedgeIndex = self.getHedgeIndex(idx)
                                    self.exitOrder(idx, instrumentID, curr_price, "EMA200Cross")
                                    infoMessage(algoName=self.algoName, message=f"EMA200cross: EXIT {symbol} @ {curr_price}")
                                    self.mylogger.log(f"EMA200cross: EXIT {symbol} @ {curr_price}")

                                    if hedgeIndex is not None:
                                        infoMessage(algoName=self.algoName, message=f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")
                                        self.mylogger.log(f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")

                                        self.exitOrder(
                                            hedgeIndex, self.idMap.get(self.openPnl.at[hedgeIndex, 'Symbol']), self.openPnl.at[hedgeIndex, 'CurrentPrice'], "HEDGE")

                                    # ReEntry
                                    self.list1_high = max(self.list1)
                                    self.PutReEntryAllow = True
                                    self.PutEntryAllow = False

                        elif symSide == "CE":
                            if (last_close > last_ema200) and (second_last_close < second_Last_ema200):
                                    hedgeIndex = self.getHedgeIndex(idx)
                                    self.exitOrder(idx, instrumentID, curr_price, "EMA200Cross")
                                    infoMessage(algoName=self.algoName, message=f"EMA200cross: EXIT {symbol} @ {curr_price}")
                                    self.mylogger.log(f"EMA200cross: EXIT {symbol} @ {curr_price}")

                                    if hedgeIndex is not None:
                                        infoMessage(algoName=self.algoName, message=f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")
                                        self.mylogger.log(f"HEDGE: EXIT {self.openPnl.at[hedgeIndex, 'Symbol']} @ {self.openPnl.at[hedgeIndex, 'CurrentPrice']}")

                                        self.exitOrder(
                                            hedgeIndex, self.idMap.get(self.openPnl.at[hedgeIndex, 'Symbol']), self.openPnl.at[hedgeIndex, 'CurrentPrice'], "HEDGE")
                                    # ReEntry
                                    self.list1_low_low = min(self.list1_low)
                                    self.CallReEntryAllow = True
                                    self.CallEntryAllow = False
                    
                    sleep(0.1)


            # --- ENTRY LOGIC ---
            if self.openPnl.empty and (candle_flag_15Min):
                if (self.PutEntryAllow):
                    if last_close > self.Twoswinghigh:
                        self.list1.append(last_high)
                        self.Tradeindexprice = last_close

                        hedgeSym = self.getHedgeSym(pe_sym, pe_price)

                        if hedgeSym:
                            hedgeData, self.idMap, self.symListConn = reconnect(
                                self.idMap, [hedgeSym])
                            hedgePrice = hedgeData[self.idMap[hedgeSym]]
                            self.entryOrder(
                                self.idMap[hedgeSym], hedgeSym,  hedgePrice, lotSize, "BUY", {"Expiry": currentExpiryDatetime}, hedge=True)
                            infoMessage(algoName=self.algoName, message=f"HedgeEntry: BUY PE {hedgeSym} @ {hedgePrice }")
                            self.mylogger.log(f"HedgeEntry: BUY PE {hedgeSym} @ {hedgePrice }")

                        self.entryOrder(self.idMap[pe_sym], pe_sym, pe_price , lotSize, "SELL", {"Expiry": currentExpiryDatetime, "IndexPrice": self.Tradeindexprice})
                        infoMessage(algoName=self.algoName, message=f"BREAKOUT: SELL PE {pe_sym} @ {pe_price }")
                        self.mylogger.log(f"BREAKOUT: SELL PE {pe_sym} @ {pe_price }")
                        self.PutEntryAllow=False
                        self.maxlist= self.maxlist[-2:]  # Keep only the last two swing highs

                if (self.CallEntryAllow):
                    if last_close < self.Twoswinglow:
                        self.list1_low.append(last_low)
                        self.Tradeindexprice = last_close

                        hedgeSym = self.getHedgeSym(ce_sym, ce_price)

                        if hedgeSym:
                            hedgeData, self.idMap, self.symListConn = reconnect(
                                self.idMap, [hedgeSym])
                            hedgePrice = hedgeData[self.idMap[hedgeSym]]
                            self.entryOrder(
                                self.idMap[hedgeSym], hedgeSym,  hedgePrice, lotSize, "BUY", {"Expiry": currentExpiryDatetime}, hedge=True)
                            infoMessage(algoName=self.algoName, message=f"HedgeEntry: BUY CE {hedgeSym} @ {hedgePrice }")
                            self.mylogger.log(f"HedgeEntry: BUY CE {hedgeSym} @ {hedgePrice }")

                        self.entryOrder(self.idMap[ce_sym], ce_sym, ce_price , lotSize, "SELL", {"Expiry": currentExpiryDatetime, "IndexPrice": self.Tradeindexprice})
                        infoMessage(algoName=self.algoName, message=f"BREAKOUT: SELL CE {ce_sym} @ {ce_price }")
                        self.mylogger.log(f"BREAKOUT: SELL CE {ce_sym} @ {ce_price }")
                        self.CallEntryAllow=False
                        self.minlist= self.minlist[-2:]  # Keep only the last two swing lows 


                if (self.PutReEntryAllow):
                    if last_close > self.list1_high:
                        self.list1.clear()
                        self.list1.append(last_high)
                        self.Tradeindexprice = last_close

                        hedgeSym = self.getHedgeSym(pe_sym, pe_price)

                        if hedgeSym:
                            hedgeData, self.idMap, self.symListConn = reconnect(
                                self.idMap, [hedgeSym])
                            hedgePrice = hedgeData[self.idMap[hedgeSym]]
                            self.entryOrder(
                                self.idMap[hedgeSym], hedgeSym,  hedgePrice, lotSize, "BUY", {"Expiry": currentExpiryDatetime}, hedge=True)
                            infoMessage(algoName=self.algoName, message=f"HedgeEntry: BUY PE {hedgeSym} @ {hedgePrice }")
                            self.mylogger.log(f"HedgeEntry: BUY PE {hedgeSym} @ {hedgePrice }")

                        self.entryOrder(self.idMap[pe_sym], pe_sym, pe_price , lotSize, "SELL", {"Expiry": currentExpiryDatetime, "IndexPrice": self.Tradeindexprice})
                        infoMessage(algoName=self.algoName, message=f"RE_ENTRY: SELL PE {pe_sym} @ {pe_price }")
                        self.mylogger.log(f"RE_ENTRY: SELL PE {pe_sym} @ {pe_price }")
                        self.PutReEntryAllow = False
                        # self.logger.info(f"[15Min] => Close: {last_close} | RSI: {last_rsi}")

                if (self.CallReEntryAllow):
                    if last_close < self.list1_low_low:
                        self.list1_low.clear()
                        self.list1.append(last_low )
                        self.Tradeindexprice = last_close

                        hedgeSym = self.getHedgeSym(ce_sym, ce_price)  

                        if hedgeSym:
                            hedgeData, self.idMap, self.symListConn = reconnect(
                                self.idMap, [hedgeSym])
                            hedgePrice = hedgeData[self.idMap[hedgeSym]]
                            self.entryOrder(
                                self.idMap[hedgeSym], hedgeSym,  hedgePrice, lotSize, "BUY", {"Expiry": currentExpiryDatetime}, hedge=True)
                            infoMessage(algoName=self.algoName, message=f"HedgeEntry: BUY CE {hedgeSym} @ {hedgePrice }")
                            self.mylogger.log(f"HedgeEntry: BUY CE {hedgeSym} @ {hedgePrice }")

                        self.entryOrder(self.idMap[ce_sym], ce_sym, ce_price , lotSize, "SELL", {"Expiry": currentExpiryDatetime, "IndexPrice": self.Tradeindexprice})
                        infoMessage(algoName=self.algoName, message=f"RE_ENTRY: SELL CE {ce_sym} @ {ce_price }")
                        self.mylogger.log(f"RE_ENTRY: SELL CE {ce_sym} @ {ce_price }")
                        self.CallReEntryAllow = False
                        
            if (candle_flag_1Min):
                if (self.BufferEntryPut):
                    # self.list1.append(last_high)
                    self.Tradeindexprice = last_close_1Min

                    hedgeSym = self.getHedgeSym(pe_sym, pe_price)

                    if hedgeSym:
                        hedgeData, self.idMap, self.symListConn = reconnect(
                            self.idMap, [hedgeSym])
                        hedgePrice = hedgeData[self.idMap[hedgeSym]]
                        self.entryOrder(
                            self.idMap[hedgeSym], hedgeSym,  hedgePrice, lotSize, "BUY", {"Expiry": currentExpiryDatetime}, hedge=True)
                        infoMessage(algoName=self.algoName, message=f"HedgeEntry: BUY PE {hedgeSym} @ {hedgePrice }")
                        self.mylogger.log(f"HedgeEntry: BUY PE {hedgeSym} @ {hedgePrice }")

                    self.entryOrder(self.idMap[pe_sym], pe_sym, pe_price , lotSize, "SELL", {"Expiry": currentExpiryDatetime, "IndexPrice": self.Tradeindexprice})
                    infoMessage(algoName=self.algoName, message=f"BufferEntry: SELL PE {pe_sym} @ {pe_price }")
                    self.mylogger.log(f"BufferEntry: SELL PE {pe_sym} @ {pe_price }")
                    self.BufferEntryPut=False
                    self.maxlist= self.maxlist[-2:]


                if (self.BufferEntryCall):
                    # self.list1_low.append(last_low )
                    self.Tradeindexprice = last_close_1Min

                    hedgeSym = self.getHedgeSym(ce_sym, ce_price)

                    if hedgeSym:
                        hedgeData, self.idMap, self.symListConn = reconnect(
                            self.idMap, [hedgeSym])
                        hedgePrice = hedgeData[self.idMap[hedgeSym]]
                        self.entryOrder(
                            self.idMap[hedgeSym], hedgeSym,  hedgePrice, lotSize, "BUY", {"Expiry": currentExpiryDatetime}, hedge=True)
                        infoMessage(algoName=self.algoName, message=f"HedgeEntry: BUY CE {hedgeSym} @ {hedgePrice }")
                        self.mylogger.log(f"HedgeEntry: BUY CE {hedgeSym} @ {hedgePrice }")

                    self.entryOrder(self.idMap[ce_sym], ce_sym, ce_price , lotSize, "SELL", {"Expiry": currentExpiryDatetime, "IndexPrice": self.Tradeindexprice})
                    infoMessage(algoName=self.algoName, message=f"BufferEntry: SELL CE {ce_sym} @ {ce_price }")
                    self.mylogger.log(f"BufferEntry: SELL CE {ce_sym} @ {ce_price }")
                    self.BufferEntryCall=False
                    self.minlist= self.minlist[-2:]

            # if (TestEntry):
            #     self.list1.clear()
            #     self.list1.append(last_high)
            #     self.Tradeindexprice = last_close

            #     hedgeSym = self.getHedgeSym(pe_sym, pe_price)

            #     if hedgeSym:
            #         hedgeData, self.idMap, self.symListConn = reconnect(
            #             self.idMap, [hedgeSym])
            #         hedgePrice = hedgeData[self.idMap[hedgeSym]]
            #         self.entryOrder(
            #             self.idMap[hedgeSym], hedgeSym,  hedgePrice, lotSize, "BUY", {"Expiry": currentExpiryDatetime}, hedge=True)
            #         infoMessage(algoName=self.algoName, message=f"HedgeEntry: BUY PE {hedgeSym} @ {hedgePrice }")
            #         self.mylogger.log.log(f"HedgeEntry: BUY PE {hedgeSym} @ {hedgePrice }")

            #     self.entryOrder(self.idMap[pe_sym], pe_sym, pe_price , lotSize, "SELL", {"Expiry": currentExpiryDatetime, "IndexPrice": self.Tradeindexprice})
            #     infoMessage(algoName=self.algoName, message=f"TEST_ENTRY: SELL PE {ce_sym} @ {ce_price }")
            #     self.mylogger.log.log(f"TEST_ENTRY: SELL PE {pe_sym} @ {pe_price }")
                
                

            if (candle_flag_15Min):
                if self.MidFlag:
                    self.Midlist.append(last_high)

                if self.MidFlag_low:
                    self.Midlist_low.append(last_low)
            
            # --- Save state periodically --- in json
            if (candle_flag_1Min):
                self.updateOpenPositionsInfra()
                self.pnlCalculator()
                self.save_flags()
                self.save_lists()
                
            sleep(0.1)


            # --- UPDATE CURRENT PRICE OF OPEN POSITIONS ---
            # if (candle_flag_1Min):
            #     self.updateOpenPrices(data)
            #     self.updateOpenPositionsInfra()
                
                
    def mainLogic(self, **kwargs):
        """Entry point for multiprocessing infra."""
        self.run()

    def save_open_positions(self):
        """Save open positions to CSV and JSON in both baseJson and baseLog folders."""
        # Save CSV in baseJson folder
        open_csv_json = os.path.join(self.fileDir["openPositions"], "open_positions.csv")
        os.makedirs(os.path.dirname(open_csv_json), exist_ok=True)
        self.openPnl.to_csv(open_csv_json, index=False)
        # Save JSON in baseJson folder
        open_json_json = os.path.join(self.fileDir["openPositions"], "open_positions.json")
        self.openPnl.to_json(open_json_json, orient="records", date_format="iso")


    def load_open_positions(self):
        """Load open positions from JSON if available."""
        open_path = os.path.join(self.fileDir["openPositions"], "open_positions.json")
        if os.path.exists(open_path):
            self.openPnl = pd.read_json(open_path)
        else:
            self.openPnl = pd.DataFrame()

    def save_closed_positions(self):
        """Save closed positions to CSV and JSON in both baseJson and baseLog folders."""
        # Save CSV in baseJson folder
        closed_csv_json = os.path.join(self.fileDir["closedPositions"], "closed_positions.csv")
        os.makedirs(os.path.dirname(closed_csv_json), exist_ok=True)
        self.closedPnl.to_csv(closed_csv_json, index=False)

        # Save CSV in baseLog folder
        closed_csv_log = os.path.join(self.fileDir["baseLog"], "ClosedPositions", "closed_positions.csv")
        os.makedirs(os.path.dirname(closed_csv_log), exist_ok=True)
        self.closedPnl.to_csv(closed_csv_log, index=False)


    def save_lists(self):
        lists = {
            "maxlist": self.maxlist,
            "minlist": self.minlist,
            "Midlist": self.Midlist,
            "Midlist_low": self.Midlist_low,
            "list1": self.list1,
            "list1_low": self.list1_low,
            "Closelist": self.Closelist,
            "Closelist_low": self.Closelist_low,
            "Twoswinghigh": self.Twoswinghigh,
            "Twoswinglow": self.Twoswinglow,
            "Tradeindexprice": self.Tradeindexprice,
            "list1_high": self.list1_high,
            "list1_low_low": self.list1_low_low
        }
        # Save in baseJson folder
        lists_path_json = os.path.join(self.fileDir["baseJson"], "lists.json")
        os.makedirs(os.path.dirname(lists_path_json), exist_ok=True)
        with open(lists_path_json, "w") as f:
            json.dump(lists, f)

    def load_lists(self):
        lists_path = os.path.join(self.fileDir["baseJson"], "lists.json")
        if os.path.exists(lists_path):
            with open(lists_path, "r") as f:
                lists = json.load(f)
                self.maxlist = lists.get("maxlist", [])
                self.minlist = lists.get("minlist", [])
                self.Midlist = lists.get("Midlist", [])
                self.Midlist_low = lists.get("Midlist_low", [])
                self.list1 = lists.get("list1", [])
                self.list1_low = lists.get("list1_low", [])
                self.Closelist = lists.get("Closelist", [])
                self.Closelist_low = lists.get("Closelist_low", [])
                self.Twoswinghigh = lists.get("Twoswinghigh", None)
                self.Twoswinglow = lists.get("Twoswinglow", None)
                self.Tradeindexprice = lists.get("Tradeindexprice", None)
                self.list1_high = lists.get("list1_high", None)
                self.list1_low_low = lists.get("list1_low_low", None)
        else:
            self.maxlist = []
            self.minlist = []
            self.Midlist = []
            self.Midlist_low = []
            self.list1 = []
            self.list1_low = []
            self.Closelist = []
            self.Closelist_low = []
            self.Twoswinghigh = None
            self.Twoswinglow = None
            self.Tradeindexprice = None
            self.list1_high = None
            self.list1_low_low = None

    def save_flags(self):
        """Save strategy flags to JSON in both baseJson and baseLog folders."""
        flags = {
            "MidFlag": self.MidFlag,
            "MidFlag_low": self.MidFlag_low,
            "PutEntryAllow": self.PutEntryAllow,
            "CallEntryAllow": self.CallEntryAllow,
            "PutReEntryAllow": self.PutReEntryAllow,
            "CallReEntryAllow": self.CallReEntryAllow,
            "flag1": self.flag1,
            "flag2": self.flag2,
            "BufferEntryPut": self.BufferEntryPut,
            "BufferEntryCall": self.BufferEntryCall,
            "BufferSignal": self.BufferSignal
        }
        # Save in baseJson folder
        flags_path_json = os.path.join(self.fileDir["baseJson"], "flags.json")
        os.makedirs(os.path.dirname(flags_path_json), exist_ok=True)
        with open(flags_path_json, "w") as f:
            json.dump(flags, f)

    def load_flags(self):
        """Load strategy flags from JSON."""
        flags_path = os.path.join(self.fileDir["baseJson"], "flags.json")
        if os.path.exists(flags_path):
            with open(flags_path, "r") as f:
                flags = json.load(f)
                self.MidFlag = flags.get("MidFlag", False)
                self.MidFlag_low = flags.get("MidFlag_low", False)
                self.PutEntryAllow = flags.get("PutEntryAllow", False)
                self.CallEntryAllow = flags.get("CallEntryAllow", False)
                self.PutReEntryAllow = flags.get("PutReEntryAllow", False)
                self.CallReEntryAllow = flags.get("CallReEntryAllow", False)
                self.flag1 = flags.get("flag1", False)
                self.flag2 = flags.get("flag2", False)
                self.BufferEntryPut= flags.get("BufferEntryPut", False)
                self.BufferEntryCall= flags.get("BufferEntryCall", False)
                self.BufferSignal= flags.get("BufferSignal", False)
        else:
            self.MidFlag = False
            self.MidFlag_low = False
            self.PutEntryAllow = False
            self.CallEntryAllow = False
            self.PutReEntryAllow = False
            self.CallReEntryAllow = False
            self.flag1 = False
            self.flag2 = False
            self.BufferEntryPut= False
            self.BufferEntryCall= False
            self.BufferSignal= False

if __name__ == "__main__":
    # For direct script run/testing
    strategy = RSIStrategy("NIFTY")
    strategy.run()  