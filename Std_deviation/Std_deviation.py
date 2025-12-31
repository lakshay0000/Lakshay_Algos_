import numpy as np
import talib as ta
import pandas as pd
import pandas_ta as taa
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getFnoBacktestData, connectToMongo

# sys.path.insert(1, '/root/backtestTools')


# Define a class algoLogic that inherits from optOverNightAlgoLogic
class algoLogic(optOverNightAlgoLogic):

    def getEquityBacktestData(self, symbol, startDateTime, endDateTime, interval, conn=None):
        """
        Retrieves backtest data i.e. range of data for a given equity symbol, start and end datetime, and interval.

        Parameters:
            symbol (string): The symbol for which backtest data is requested.

            startDateTime (float or datetime): The start datetime for the backtest data.

            endDateTime (float or datetime): The end datetime for the backtest data.

            interval (string): The resampling interval for the data.

        Returns:
            DataFrame: A pandas DataFrame containing resampled backtest data.
        """
        try:
            if isinstance(startDateTime, datetime) and isinstance(startDateTime, datetime):
                startTimestamp = startDateTime.timestamp()
                endTimestamp = endDateTime.timestamp()
            elif isinstance(startDateTime, int) and isinstance(startDateTime, int):
                startTimestamp = float(startDateTime)
                endTimestamp = float(endDateTime)
            elif isinstance(startDateTime, float) and isinstance(startDateTime, float):
                startTimestamp = startDateTime
                endTimestamp = endDateTime
            else:
                raise Exception(
                    "startDateTime or endDateTime is not a timestamp(float or int) or datetime object"
                )

            if conn is None:
                conn = connectToMongo()

        
            db = conn["STOCK_MINUTE_1"]
            collection = db.Data

            rec = collection.find({"$and": [{"sym": symbol}, {
                                "ti": {"$gte": startTimestamp, "$lte": endTimestamp}},]})
            rec = list(rec)

            if rec:
                df = pd.DataFrame(rec)

                if 'oi' in df.columns:
                    df['oi'] = df["oi"].fillna(0)
                else:
                    df['oi'] = 0

                if 'v' in df.columns:
                    df['v'] = df["v"].fillna(0)
                else:
                    df['v'] = 0

                if 'date' in df.columns:
                    df['date'] = df["date"].fillna(pd.to_datetime(df["ti"]))

                # df.dropna(inplace=True)
                df.drop_duplicates(subset="ti", inplace=True)
                df.sort_values(by=["ti"], inplace=True, ascending=True)
                df.set_index("ti", inplace=True)

                df.index = pd.to_datetime(df.index, unit="s")
                df.index = df.index + timedelta(hours=5, minutes=30)

                # if interval[-1:] == "D":
                #     df_resample = df.resample(interval).agg({
                #         "o": "first",
                #         "h": "max",
                #         "l": "min",
                #         "c": "last",
                #         "v": "sum",
                #         "oi": "sum",
                #     })

                df = df.between_time("09:16:00", "15:29:00")
                df_resample = df.resample(interval, origin="9:16").agg(
                    {"o": "first",
                    "h": "max",
                    "l": "min",
                    "c": "last",
                    "v": "sum",
                    "oi": "sum", }
                )

                df_resample.index = (df_resample.index.values.astype(np.int64) //
                                    10**9) - 19800
                df_resample.insert(0, "ti", df_resample.index)

                df_resample.dropna(inplace=True)

                datetimeCol = pd.to_datetime(df_resample.index, unit="s")
                datetimeCol = datetimeCol + timedelta(hours=5, minutes=30)
                df_resample.insert(loc=1, column="datetime", value=datetimeCol)

                return df_resample

        except Exception as e:
            raise Exception(e)

    def get_daily_top_bottom_stocks(self, stock_list, openEpoch, target_time_epoch, stock_1min_data, dict_1d=None, TradeType=0):
        """
        Calculates percentage change for each stock between 9:15 epoch and target_time in epoch.
        Uses pre-fetched 1-min data from stock_1min_data dict.
        Returns top 5 and bottom 5 stocks by percentage change.
        """
        # target_time_dt = datetime.fromtimestamp(target_time_epoch)
        # target_time = target_time_dt.time()



        pct_changes = []
        if TradeType == 0:
            for stock in stock_list:
                df = stock_1min_data.get(stock)
                if df is None or df.empty:
                    self.strategyLogger.info(f"No data for {stock} on {openEpoch}")
                    continue
                # # Ensure datetime column is in datetime format
                # # df['datetime'] = pd.to_datetime(df['datetime'])
                # open915 = df[df['datetime'].dt.time == time(9, 15)]
                # target_row = df[df['datetime'].dt.time == target_time]
                # if open915.empty or target_row.empty:
                #     continue
                if (target_time_epoch not in df.index) or (openEpoch not in df.index):
                    continue
                
                price915 = df.at[openEpoch, 'c']
                price_target = df.at[target_time_epoch, 'c']
                pct_change = ((price_target - price915) / price915) * 100
                pct_changes.append((stock, pct_change))

        elif TradeType == 1:
            for stock in stock_list:
                df = stock_1min_data.get(stock)
                df_1d = dict_1d.get(stock)
                if df is None or df.empty or df_1d is None or df_1d.empty:
                    self.strategyLogger.info(f"No data for {stock} on {openEpoch}")
                    continue
                # # Ensure datetime column is in datetime format
                # # df['datetime'] = pd.to_datetime(df['datetime'])
                # open915 = df[df['datetime'].dt.time == time(9, 15)]
                # target_row = df[df['datetime'].dt.time == target_time]
                # if open915.empty or target_row.empty:
                #     continue
                if (target_time_epoch not in df.index) or (openEpoch not in df_1d.index):
                    continue
                
                price915 = df_1d.at[openEpoch, 'c']
                price_target = df.at[target_time_epoch, 'c']
                pct_change = ((price_target - price915) / price915) * 100
                pct_changes.append((stock, pct_change))

        # Sort by percentage change
        pct_changes_sorted = sorted(pct_changes, key=lambda x: x[1], reverse=True)
        top5 = [x[0] for x in pct_changes_sorted[:10]]
        Perc_top5 = [x[1] for x in pct_changes_sorted[:10]]
        bottom5 = [x[0] for x in pct_changes_sorted[-10:]]
        Perc_bottom5 = [x[1] for x in pct_changes_sorted[-10:]]
        return top5, bottom5, pct_changes_sorted, Perc_top5, Perc_bottom5

    # Define a method to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):
        conn = connectToMongo()

        # Read your stock list
        with open("/root/Lakshay_Algos/stocksList/Std_Deviation.md") as f:
            stock_list = [line.strip() for line in f if line.strip()]


        # Add necessary columns to the DataFrame
        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()
        

        try:
            # Fetch historical data for backtesting
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min", conn=conn)
            # df_1d = getFnoBacktestData(indexSym, startEpoch-(86400*50), endEpoch, "1D", conn=conn)
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        # df_1d.dropna(inplace=True)

        # df_1d['ATR'] = df_1d['h'] - df_1d['l']
        # df_1d['ATR%'] = (df_1d['ATR']/df_1d['o']) * 100
        # df_1d['N_ATR%'] = df_1d['ATR%']
        # df_1d.loc[df_1d['c'] < df_1d['o'], 'N_ATR%'] = -df_1d.loc[df_1d['c'] < df_1d['o'], 'ATR%']  
        # mean_atr_percent = df_1d['N_ATR%'].mean()
        # df_1d['ATR%mean'] = mean_atr_percent

        # mean_neg_n_atr_percent = df_1d.loc[df_1d['N_ATR%'] < 0, 'N_ATR%'].mean()
        # mean_pos_n_atr_percent = df_1d.loc[df_1d['N_ATR%'] > 0, 'N_ATR%'].mean()

        # df_1d['N_ATR%_mean_neg'] = mean_neg_n_atr_percent
        # df_1d['N_ATR%_mean_pos'] = mean_pos_n_atr_percent


        df.to_csv(
                f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Min.csv")
        # df_1d.to_csv(
        #         f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1d.csv"
        #     ) 

        


        stock_1min_data = {}
        stock_1d_data = {}
        stock_state = {}
        analysis_data = []

        for stock in stock_list:

            try:
                # Fetch historical data for backtesting
                # df_1min = self.getEquityBacktestData(stock, startEpoch-(86400*50), endEpoch, "1Min", conn=conn)
                df_1d = self.getEquityBacktestData(stock , startEpoch, endEpoch, "1D", conn=conn)
            except Exception as e:
                # Log an exception if data retrieval fails
                self.strategyLogger.info(
                    f"Data not found for {baseSym} in range {startDate} to {endDate}")
                raise Exception(e)
            
            if df_1d is None:
                self.strategyLogger.info(f"No data for {stock} in 1Min or 1D timeframe.")
                continue

            # Drop rows with missing values
            df_1d.dropna(inplace=True)

            # Calculate the 20-period EMA
            # df_1min['EMA10'] = df_1min['c'].ewm(span=10, adjust=False).mean()

            # df_1min = df_1min[df_1min.index >= startEpoch]

            # # Determine crossover signals
            # df_1min["EMADown"] = np.where((df_1min["EMA10"] < df_1min["EMA10"].shift(1)), 1, 0)
            # df_1min["EMAUp"] = np.where((df_1min["EMA10"] > df_1min["EMA10"].shift(1)), 1, 0)

            df_1d['Return'] = ((df_1d['c'] - df_1d['c'].shift(1)) / df_1d['c'].shift(1)) * 100
            Std_dev = df_1d['Return'].std()
            df_1d['Std_Dev'] = Std_dev

            analysis_data.append({
                'stockname': stock,
                'Std_dev': Std_dev
            })

            
            # df_1d['std'] = df_1d['SMA_10'].rolling(window=10).std()

            # Add 33360 to the index to match the timestamp
            # df_1d.index = df_1d.index + 33360
            # df_1d.ti = df_1d.ti + 33360

            # stock_1min_data[stock] = df_1min
            stock_1d_data[stock] = df_1d
            stock_state[stock] = {
                # "m_upper": None,
                # "m_lower": None,
                # "i": 0,
                # "k": 0,
                # "j": 0,
                # "Interval": 0,
                # "high_list": [],
                # "low_list": [],
                # "max_list": [],
                # "min_list": [],
                # "high_list_Interval": [],
                # "low_list_Interval": [],
                # "Today_high": None,
                # "Today_low": None,
                # "prev_DH": None,
                # "prev_DL": None,
                "stockcount": None,
                "main_trade": True,
                "TradeLimit": 0,
                "high_list": [],
                "low_list": [],
                "High": None,
                "Low": None,
                "Range": None,
                "SecondTrade": False,
            }


            # df_1min.to_csv(
            #     f"{self.fileDir['backtestResultsCandleData']}{stock}_1Min.csv")
            df_1d.to_csv(
                f"{self.fileDir['backtestResultsCandleData']}{stock}_1d.csv"
            )

        # Sort analysis_data by abs_mean_atr_percent in descending order
        analysis_data = sorted(analysis_data, key=lambda x: x['Std_dev'], reverse=True)

        # After the for loop:
        df_analysis = pd.DataFrame(analysis_data)
        output_path = r"/root/Lakshay_Algos/Std_deviation/Yearly_Std_dev.csv"
        df_analysis.to_csv(output_path, index=False)
        


        lastIndexTimeData = [0, 0] 

        # Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        # expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        # expiryEpoch= expiryDatetime.timestamp()
        # lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        amountPerTrade = 500
        New_iteration = False

        

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 
            New_iteration = True

            for stock in stock_list:

                state = stock_state[stock]

                # Then, inside your main loop, use:
                df_1min = stock_1min_data.get(stock)
                df_1d = stock_1d_data.get(stock)
                if df_1min is None:
                    continue

                self.timeData = float(timeData)
                self.humanTime = datetime.fromtimestamp(timeData)
                print(self.humanTime)

                # # Skip the dates 2nd March 2024 and 18th May 2024
                # if self.humanTime.date() == datetime(2024, 3, 2).date() or self.humanTime.date() == datetime(2024, 2, 15).date():
                #     continue

                # Skip time periods outside trading hours
                if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                    continue

                # Update lastIndexTimeData
                lastIndexTimeData.pop(0)
                lastIndexTimeData.append(timeData-60)

                # Strategy Specific Trading Time
                if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 20)):
                    continue

                if lastIndexTimeData[1] not in df_1min.index:
                    continue

                #  # Log relevant information
                # if lastIndexTimeData[1] in df.index:
                #     self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")

                # if (self.humanTime.time() == time(9, 16)):
                #     state["Sell_Breakout"]=None
                #     state["Buy_Breakout"]=None
                
                # prev_day = timeData - 86400
                if (self.humanTime.time() == time(9, 16)):
                    prev_day = timeData - 86400
                    # state["m_upper"] = None
                    # state["m_lower"] = None
                    state["main_trade"] = True
                    state["TradeLimit"] = 0
                    state["high_list"] = []
                    state["low_list"] = []
                    state["High"] = None
                    state["Low"] = None
                    state["Range"] = None
                    state["SecondTrade"] = False
                    openEpoch = lastIndexTimeData[1]
                    main_stock_list = stock_list
                    stock_merged = []
                    # with open("/root/Lakshay_Algos/stocksList/nifty50.md") as f:
                    #     stock_list = [line.strip() for line in f if line.strip()]
                    
                    #check if previoud day exists in 1d data
                    while prev_day not in df_1d.index:
                        prev_day = prev_day - 86400


                if (self.humanTime.time() > time(9, 16)) and (self.humanTime.time() <= time(9, 21)):
                    state["high_list"].append(df_1min.at[lastIndexTimeData[1], "h"])
                    state["low_list"].append(df_1min.at[lastIndexTimeData[1], "l"])
                    if (self.humanTime.time() == time(9, 21)):
                        state["High"] = max(state["high_list"])
                        state["Low"] = min(state["low_list"])
                        state["Range"] = state["High"]-state["Low"]
                        self.strategyLogger.info(f"{self.humanTime} {stock} Range: {state['Range']} High: {state['High']} Low: {state['Low']}")

                # Update current price for open positions
                if not self.openPnl.empty:
                    for index, row in self.openPnl.iterrows():
                        if lastIndexTimeData[1] in df_1min.index:
                            symSide = row["Symbol"]
                            if symSide == stock:
                                self.openPnl.at[index,"CurrentPrice"] = df_1min.at[lastIndexTimeData[1], "c"]

                # Calculate and update PnL
                self.pnlCalculator()



                #Updating daily index
                # prev_day = timeData - 86400
                # if (self.humanTime.time() == time(9, 16)):
                #     # Today_open = df_1min.at[lastIndexTimeData[1], 'o']
                #     # state["Today_high"] = df_1min.at[lastIndexTimeData[1], 'h']
                #     # state["Today_low"]  = df_1min.at[lastIndexTimeData[1], 'l']
                #     #check if previoud day exists in 1d data
                #     while prev_day not in df_1d.index:
                #         prev_day = prev_day - 86400

                # if prev_day in df_1d.index:
                #     state["prev_DH"] = (df_1d.at[prev_day, 'h'])
                #     state["prev_DL"] = (df_1d.at[prev_day, 'l'])  
                #     self.strategyLogger.info(f"{self.humanTime} Previous Day High: {state['prev_DH']}, Previous Day Low: {state['prev_DL']}, BarNo: {375 + state['i']}")

                # if state["m_upper"] is None and state["m_lower"] is None:
                #     state["m_upper"] = (state["Today_high"] - state["prev_DH"]) / (375)
                #     state["m_lower"] = (state["Today_low"]  - state["prev_DL"]) / (375)
                #     self.strategyLogger.info(f"{self.humanTime} Slope Upper: {state['m_upper']}, Slope Lower: {state['m_lower']}")  

                # if lastIndexTimeData[1] in df_1min.index:
                #     BarNo = 375 + state["i"]+ state["k"]
                #     upper_ray = state["prev_DH"] + (state["m_upper"] * BarNo)
                #     lower_ray = state["prev_DL"] + (state["m_lower"] * BarNo) 
                #     state["i"] = state["i"]+ 1
                #     self.strategyLogger.info(f"{self.humanTime} Upper Ray: {upper_ray}, Lower Ray: {lower_ray}, BarNo: {BarNo}")
                #     state["high_list"].append(df_1min.at[lastIndexTimeData[1], "h"])
                #     state["low_list"].append(df_1min.at[lastIndexTimeData[1], "l"])
                #     state["high_list_Interval"].append(df_1min.at[lastIndexTimeData[1], "h"])
                #     state["low_list_Interval"].append(df_1min.at[lastIndexTimeData[1], "l"])

                # if upper_ray < lower_ray:
                #     temp = upper_ray
                #     a = lower_ray
                #     lower_ray = temp

                # if state["i"]== 60:
                #     state["m_upper"] = None
                #     state["m_lower"] = None
                #     state["i"]= 0
                #     state["k"] = state["k"] + 60

                #     state["max_list"].append(max(state["high_list_Interval"]))
                #     state["min_list"].append(min(state["low_list_Interval"]))

                #     if len(state["max_list"]) < 3:

                #         state["Today_high"] = max(state["high_list"])
                #         high_index = state["high_list"].index(state["Today_high"])
                #         state["Today_low"]  = min(state["low_list"])
                #         low_index = state["low_list"].index(state["Today_low"] )

                #     else:
                #         # Consider last two max values for Today_high
                #         last_two_max = state["max_list"][-2:]
                #         state["Today_high"] = max(last_two_max)
                #         high_index = len(state["high_list"]) - 1 - state["high_list"][::-1].index(state["Today_high"])

                #         # Consider last two min values for Today_low
                #         last_two_min = state["min_list"][-2:]
                #         state["Today_low"] = min(last_two_min)
                #         low_index = len(state["low_list"]) - 1 - state["low_list"][::-1].index(state["Today_low"])

                    
                #     state["high_list_Interval"] = []
                #     state["low_list_Interval"] = [] 

                #     state["m_upper"] = (state["Today_high"] - state["prev_DH"]) / (375+high_index)
                #     state["m_lower"] = (state["Today_low"]  - state["prev_DL"]) / (375+low_index)

                #     self.strategyLogger.info(f"{self.humanTime} 1 Hour Completed. High List: {state['Today_high']}, Low List: {state['Today_low']}, stock: {stock}")
                #     self.strategyLogger.info(f"{self.humanTime} New Slope Upper: {state['m_upper']}, New Slope Lower: {state['m_lower']}")
                

                # Check for exit conditions and execute exit orders
                if not self.openPnl.empty:
                    for index, row in self.openPnl.iterrows():

                        symSide = row["Symbol"]
                        # symSide = symSide[len(symSide) - 2:]   
                        # print("open_stock", symSide)  
                        # print("current_stock", stock) 
                             
                        if self.humanTime.time() >= time(15, 20):
                            exitType = "Time Up"
                            self.exitOrder(index, exitType)

                        elif symSide == stock:
                            if (row["PositionStatus"]==1):
                                if (df_1min.at[lastIndexTimeData[1], 'EMA10'] < row["Stoploss"]) and (df_1min.at[lastIndexTimeData[1], 'c'] < row["Stoploss"]):
                                    exitType = "Stoploss Lower half Range Hit"
                                    self.exitOrder(index, exitType)
                                    state["SecondTrade"] = True


                                elif row["CurrentPrice"] >= row["Target"]:
                                    exitType = "Target Hit"
                                    self.exitOrder(index, exitType)


                            elif (row["PositionStatus"]==-1):
                                if (df_1min.at[lastIndexTimeData[1], 'EMA10'] > row["Stoploss"]) and (df_1min.at[lastIndexTimeData[1], 'c'] > row["Stoploss"]):
                                    exitType = "Stoploss Upper half Range Hit"
                                    self.exitOrder(index, exitType)
                                    state["SecondTrade"] = True


                                elif row["CurrentPrice"] <= row["Target"]:
                                    exitType = "Target Hit"
                                    self.exitOrder(index, exitType)


                tradecount = self.openPnl['Symbol'].value_counts()
                state["stockcount"]= tradecount.get(stock, 0)
                check_times = [time(9, 21), time(9, 26), time(9, 31)]

                if (self.humanTime.time() in check_times) and (self.humanTime.time() < time(15, 20)) and New_iteration:

                    top5, bottom5, pct_changes_sorted, Perc_top5, Perc_bottom5 = self.get_daily_top_bottom_stocks(main_stock_list, openEpoch, lastIndexTimeData[1], stock_1min_data, dict_1d=stock_1d_data, TradeType=0)

                    selected_stocks = top5 + bottom5
                    stock_merged = list(dict.fromkeys(selected_stocks + stock_merged))

                    if self.humanTime.time() == time(9, 31):
                       stock_list = stock_merged
                       self.strategyLogger.info(f"StockTraded :- {stock_merged}")
                       self.strategyLogger.info(f"No_StockTraded :- {len(stock_merged)}")

                    # top_merged = list(dict.fromkeys(top5 + top_merged))
                    # bottom_merged = list(dict.fromkeys(bottom5 + bottom_merged))
                    
                    self.strategyLogger.info(f"{self.humanTime} Gainers: {top5}, Losers: {bottom5}")
                    self.strategyLogger.info(f"{self.humanTime} Gainers %: {Perc_top5}, Losers %: {Perc_bottom5}")
                    # self.strategyLogger.info(f"{self.humanTime} Top Merged: {top_merged}, Bottom Merged: {bottom_merged}")
                    New_iteration = False


                # Refresh stock list at 15:20
                if (self.humanTime.time() < time(9, 22)):
                    continue


                if (self.humanTime.time() == time(15, 20)) and New_iteration:
                    with open("/root/Lakshay_Algos/stocksList/nifty50.md") as f:
                        stock_list = [line.strip() for line in f if line.strip()]

                    New_iteration = False



                # Check for entry signals and execute orders
                if ((timeData-60) in df_1min.index) and (self.humanTime.time() < time(15, 20)):

                    if (stock in stock_merged) and state["main_trade"]:
                        if (df_1min.at[lastIndexTimeData[1], "c"] < state["Low"]):

                            entry_price = df_1min.at[lastIndexTimeData[1], "c"]
                            buffer= (state["Low"] - entry_price) + state["Range"]
                            target= entry_price - buffer
                            stoploss = entry_price + (buffer/2)

                            self.entryOrder(entry_price, stock, (amountPerTrade//buffer), "SELL", {"Target": target, "Stoploss": stoploss})
                            state["main_trade"] = False
                            state["TradeLimit"] = state["TradeLimit"]+1


                        if (df_1min.at[lastIndexTimeData[1], "c"] > state["High"]):

                            entry_price = df_1min.at[lastIndexTimeData[1], "c"]
                            buffer = (entry_price - state["High"]) + state["Range"]
                            target = entry_price + buffer
                            stoploss = entry_price - (buffer/2)

                            self.entryOrder(entry_price, stock, (amountPerTrade//buffer), "BUY", {"Target": target, "Stoploss": stoploss})
                            state["main_trade"] = False
                            state["TradeLimit"] = state["TradeLimit"]+1 

                    if (stock in stock_merged) and state["SecondTrade"] and (state["TradeLimit"]<3):
                        if (df_1min.at[lastIndexTimeData[1], 'EMA10'] < state["Low"]) and (df_1min.at[lastIndexTimeData[1], 'c'] < state["Low"]):

                            entry_price = df_1min.at[lastIndexTimeData[1], "c"]
                            buffer= (state["Low"] - entry_price) + state["Range"]
                            target= entry_price - buffer
                            stoploss = entry_price + (buffer/2)

                            self.entryOrder(entry_price, stock, (amountPerTrade//buffer), "SELL", {"Target": target, "Stoploss": stoploss})
                            state["SecondTrade"] = False
                            state["TradeLimit"] = state["TradeLimit"]+1


                        if (df_1min.at[lastIndexTimeData[1], 'EMA10'] > state["High"]) and (df_1min.at[lastIndexTimeData[1], 'c'] > state["High"]):

                            entry_price = df_1min.at[lastIndexTimeData[1], "c"]
                            buffer = (entry_price - state["High"]) + state["Range"]
                            target = entry_price + buffer
                            stoploss = entry_price - (buffer/2)

                            self.entryOrder(entry_price, stock, (amountPerTrade//buffer), "BUY", {"Target": target, "Stoploss": stoploss})
                            state["SecondTrade"] = False
                            state["TradeLimit"] = state["TradeLimit"]+1 



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
    startDate = datetime(2025, 1, 1, 9, 15)
    endDate = datetime(2025, 12, 25, 15, 30)

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