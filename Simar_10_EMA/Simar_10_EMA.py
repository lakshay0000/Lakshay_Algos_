import numpy as np
import talib as ta
import pandas as pd
import pandas_ta as taa
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getEquityBacktestData, getFnoBacktestData, connectToMongo

# sys.path.insert(1, '/root/backtestTools')


# Define a class algoLogic that inherits from optOverNightAlgoLogic
class algoLogic(optOverNightAlgoLogic):

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
        with open("/root/Lakshay_Algos/stocksList/nifty50.md") as f:
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
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)

        df.to_csv(
                f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Min.csv")


        stock_1min_data = {}
        stock_1d_data = {}
        stock_state = {}

        for stock in stock_list:

            try:
                # Fetch historical data for backtesting
                df_1min = getEquityBacktestData(stock, startEpoch-(86400*50), endEpoch, "1Min", conn=conn)
                df_1d = getEquityBacktestData(stock , startEpoch-(86400*50), endEpoch, "1D", conn=conn)
            except Exception as e:
                # Log an exception if data retrieval fails
                self.strategyLogger.info(
                    f"Data not found for {baseSym} in range {startDate} to {endDate}")
                raise Exception(e)

            # Drop rows with missing values
            df_1min.dropna(inplace=True)
            df_1d.dropna(inplace=True)

            # Calculate the 20-period EMA
            df_1min['EMA10'] = df_1min['c'].ewm(span=10, adjust=False).mean()

            df_1min = df_1min[df_1min.index >= startEpoch]

            # Determine crossover signals
            df_1min["EMADown"] = np.where((df_1min["EMA10"] < df_1min["EMA10"].shift(1)), 1, 0)
            df_1min["EMAUp"] = np.where((df_1min["EMA10"] > df_1min["EMA10"].shift(1)), 1, 0)

            # Add 33360 to the index to match the timestamp
            df_1d.index = df_1d.index + 33360
            df_1d.ti = df_1d.ti + 33360

            df_1d = df_1d[df_1d.index >= ((startEpoch-86340)-(86400*5))]

            stock_1min_data[stock] = df_1min
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
                "Sell_Breakout": None,
                "Buy_Breakout": None
            }


            df_1min.to_csv(
                f"{self.fileDir['backtestResultsCandleData']}{stock}_1Min.csv")
            df_1d.to_csv(
                f"{self.fileDir['backtestResultsCandleData']}{stock}_1d.csv"
            )
        


        lastIndexTimeData = [0, 0]

        # Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        # expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        # expiryEpoch= expiryDatetime.timestamp()
        # lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        amountPerTrade = 100000
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

                if (self.humanTime.time() == time(9, 16)):
                    state["Sell_Breakout"]=None
                    state["Buy_Breakout"]=None

                
                # prev_day = timeData - 86400
                if (self.humanTime.time() == time(9, 16)) and New_iteration:
                    prev_day = timeData - 86400
                    # state["m_upper"] = None
                    # state["m_lower"] = None
                    # state["i"] = 0
                    # state["k"] = 0
                    # state["j"] = 0
                    # state["high_list"] = []
                    # state["low_list"] = []
                    # state["high_list_Interval"] = []
                    # state["low_list_Interval"] = [] 
                    # state["max_list"] = []
                    # state["min_list"] = []
                    top_merged = []
                    bottom_merged = []
                    openEpoch = lastIndexTimeData[1]
                    # with open("/root/Lakshay_Algos/stocksList/nifty50.md") as f:
                    #     stock_list = [line.strip() for line in f if line.strip()]
                    
                    #check if previoud day exists in 1d data
                    while prev_day not in df_1d.index:
                        prev_day = prev_day - 86400

                    top5, bottom5, pct_changes_sorted, Perc_top5, Perc_bottom5 = self.get_daily_top_bottom_stocks(stock_list, prev_day, lastIndexTimeData[1], stock_1min_data, dict_1d=stock_1d_data, TradeType=1)

                    selected_stocks = top5 + bottom5
                    stock_list = selected_stocks

                    top_merged = list(dict.fromkeys(top5 + top_merged))
                    bottom_merged = list(dict.fromkeys(bottom5 + bottom_merged))
                    
                    self.strategyLogger.info(f"{self.humanTime} Gainers: {top5}, Losers: {bottom5}")
                    self.strategyLogger.info(f"{self.humanTime} Gainers %: {Perc_top5}, Losers %: {Perc_bottom5}")
                    self.strategyLogger.info(f"{self.humanTime} Top Merged: {top_merged}, Bottom Merged: {bottom_merged}")

                    New_iteration = False

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
                                if row["CurrentPrice"] >= 1.02*row["EntryPrice"]:
                                    exitType = "Target Hit"
                                    self.exitOrder(index, exitType)
                                    stock_list.remove(stock)
                                    if stock in top_merged:
                                        top_merged.remove(stock)
                                    if stock in bottom_merged:
                                        bottom_merged.remove(stock)

                                elif row["CurrentPrice"] <= 0.98*row["EntryPrice"]:
                                    exitType = "Stoploss Hit"
                                    self.exitOrder(index, exitType)
                                    stock_list.remove(stock)
                                    if stock in top_merged:
                                        top_merged.remove(stock)
                                    if stock in bottom_merged:
                                        bottom_merged.remove(stock)


                            elif (row["PositionStatus"]==-1):
                                if row["CurrentPrice"] <= 0.98*row["EntryPrice"]:
                                    exitType = "Target Hit"
                                    self.exitOrder(index, exitType)
                                    stock_list.remove(stock)
                                    if stock in top_merged:
                                        top_merged.remove(stock)
                                    if stock in bottom_merged:
                                        bottom_merged.remove(stock)

                                elif row["CurrentPrice"] >= 1.02*row["EntryPrice"]:
                                    exitType = "Stoploss Hit"
                                    self.exitOrder(index, exitType)
                                    stock_list.remove(stock)
                                    if stock in top_merged:
                                        top_merged.remove(stock)
                                    if stock in bottom_merged:
                                        bottom_merged.remove(stock)
                        
                            # elif (row["PositionStatus"]==1) and df_1min.at[lastIndexTimeData[1], "Supertrend"] == -1:
                            #     exitType = "Lower Ray Hit"
                            #     self.exitOrder(index, exitType)

                            #     if stock in bottom5:

                            #         entry_price = df_1min.at[lastIndexTimeData[1], "c"]

                            #         self.entryOrder(entry_price, stock, (amountPerTrade//entry_price), "SELL")

                                    # if self.humanTime.time() < time(10, 15):
                                    #     state["Today_high"] = max(state["high_list"])
                                    #     high_index = state["high_list"].index(state["Today_high"])
                                    #     state["m_upper"] = (state["Today_high"] - state["prev_DH"]) / (375+high_index)


                            # elif (row["PositionStatus"]==-1) and df_1min.at[lastIndexTimeData[1], "Supertrend"] == 1:
                            #     exitType = "Upper Ray Hit"
                            #     self.exitOrder(index, exitType)

                            #     if stock in top5:

                            #         entry_price = df_1min.at[lastIndexTimeData[1], "c"] 

                            #         self.entryOrder(entry_price, stock, (amountPerTrade//entry_price), "BUY")

                                    # if self.humanTime.time() < time(10, 15):
                                    #     state["Today_low"] = min(state["low_list"])
                                    #     low_index = state["low_list"].index(state["Today_low"])
                                    #     state["m_lower"] = (state["Today_low"] - state["prev_DL"]) / (375+low_index)


                tradecount = self.openPnl['Symbol'].value_counts()
                state["stockcount"]= tradecount.get(stock, 0)

                if state["Sell_Breakout"] is None:
                    if (df_1min.at[lastIndexTimeData[1], "c"] > df_1min.at[lastIndexTimeData[1], "EMA10"]) and (df_1min.at[lastIndexTimeData[1], "o"] > df_1min.at[lastIndexTimeData[1], "EMA10"]):
                        if (df_1min.at[lastIndexTimeData[1], "h"] > df_1min.at[lastIndexTimeData[1], "EMA10"]) and (df_1min.at[lastIndexTimeData[1], "l"] > df_1min.at[lastIndexTimeData[1], "EMA10"]):
                            state["Sell_Breakout"] = df_1min.at[lastIndexTimeData[1], "l"]
                
                if state["Buy_Breakout"] is None:
                    if (df_1min.at[lastIndexTimeData[1], "c"] < df_1min.at[lastIndexTimeData[1], "EMA10"]) and (df_1min.at[lastIndexTimeData[1], "o"] < df_1min.at[lastIndexTimeData[1], "EMA10"]):
                        if (df_1min.at[lastIndexTimeData[1], "h"] < df_1min.at[lastIndexTimeData[1], "EMA10"]) and (df_1min.at[lastIndexTimeData[1], "l"] < df_1min.at[lastIndexTimeData[1], "EMA10"]):
                            state["Buy_Breakout"] = df_1min.at[lastIndexTimeData[1], "h"]

                # if (self.humanTime.minute % 5 == 0) and (self.humanTime.time() < time(15, 20)) and New_iteration:

                #     top5, bottom5, pct_changes_sorted, Perc_top5, Perc_bottom5 = self.get_daily_top_bottom_stocks(stock_list, openEpoch, lastIndexTimeData[1], stock_1min_data)

                #     selected_stocks = top5 + bottom5

                #     top_merged = list(dict.fromkeys(top5 + top_merged))
                #     bottom_merged = list(dict.fromkeys(bottom5 + bottom_merged))
                    
                #     self.strategyLogger.info(f"{self.humanTime} Gainers: {top5}, Losers: {bottom5}")
                #     self.strategyLogger.info(f"{self.humanTime} Gainers %: {Perc_top5}, Losers %: {Perc_bottom5}")
                #     self.strategyLogger.info(f"{self.humanTime} Top Merged: {top_merged}, Bottom Merged: {bottom_merged}")
                #     New_iteration = False


                # Refresh stock list at 15:20
                if (self.humanTime.time() == time(15, 20)) and New_iteration:
                    with open("/root/Lakshay_Algos/stocksList/nifty50.md") as f:
                        stock_list = [line.strip() for line in f if line.strip()]

                    New_iteration = False



                # Check for entry signals and execute orders
                if ((timeData-60) in df_1min.index) and (self.humanTime.time() < time(15, 20)):

                    if (stock in top_merged) and (state["stockcount"] ==0) and state["Sell_Breakout"] is not None:
                        if (df_1min.at[lastIndexTimeData[1], "EMADown"] == 1) and (df_1min.at[lastIndexTimeData[1], "c"]<state["Sell_Breakout"]):

                            entry_price = df_1min.at[lastIndexTimeData[1], "c"]

                            self.entryOrder(entry_price, stock, (amountPerTrade//entry_price), "SELL")

                
                    if (stock in bottom_merged) and (state["stockcount"] ==0) and state["Buy_Breakout"] is not None:
                        if (df_1min.at[lastIndexTimeData[1], "EMAUp"] == 1) and (df_1min.at[lastIndexTimeData[1], "c"]>state["Buy_Breakout"]):

                            entry_price = df_1min.at[lastIndexTimeData[1], "c"]

                            self.entryOrder(entry_price, stock, (amountPerTrade//entry_price), "BUY")


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
    endDate = datetime(2025, 8, 30, 15, 30)

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