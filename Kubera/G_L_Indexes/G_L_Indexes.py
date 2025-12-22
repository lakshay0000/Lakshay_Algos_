import numpy as np
import os
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

    def get_daily_top_bottom_stocks(self, stock_list, openEpoch, target_time_epoch, stock_1min_data, G_L = True):
        """
        Calculates percentage change for each stock between 9:15 epoch and target_time in epoch.
        Uses pre-fetched 1-min data from stock_1min_data dict.
        Returns top 5 and bottom 5 stocks by percentage change.
        """
        # target_time_dt = datetime.fromtimestamp(target_time_epoch)
        # target_time = target_time_dt.time()

        pct_changes = []
        only_pct_changes = []
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
                self.strategyLogger.info(f"Missing data for {stock} at required times on {openEpoch}")  
                continue
            
            price915 = df.at[openEpoch, 'c']
            price_target = df.at[target_time_epoch, 'c']
            pct_change = ((price_target - price915) / price915) * 100
            pct_changes.append((stock, pct_change))
            only_pct_changes.append(pct_change) 

        # Sort by percentage change
        if G_L == True:
            pct_changes_sorted = sorted(pct_changes, key=lambda x: x[1], reverse=True)
            top5 = [x[0] for x in pct_changes_sorted[:10]]
            Perc_top5 = [x[1] for x in pct_changes_sorted[:10]]
            bottom5 = [x[0] for x in pct_changes_sorted[-10:]]
            Perc_bottom5 = [x[1] for x in pct_changes_sorted[-10:]]
            return top5, bottom5, pct_changes_sorted, Perc_top5, Perc_bottom5

        else:
            return only_pct_changes

    # Define a method to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):
        conn = connectToMongo()

        # Read your stock list
        with open("/root/Lakshay_Algos/stocksList/Stock_Research.md") as f:
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
        # stock_1d_data = {}
        stock_state = {}

        for stock in stock_list:

            try:
                # Fetch historical data for backtesting
                df_1min = getEquityBacktestData(stock, startEpoch-(86400*50), endEpoch, "1Min", conn=conn)
                # df_1d = getEquityBacktestData(stock , startEpoch-(86400*50), endEpoch, "1D", conn=conn)
            except Exception as e:
                # Log an exception if data retrieval fails
                self.strategyLogger.info(
                    f"Data not found for {stock} in range {startDate} to {endDate}")
                raise Exception(e)
            
            
            if df_1min is None or df_1min.empty:
                self.strategyLogger.info(
                    f"No data for {stock} in range {startDate} to {endDate}")
                continue

            # Drop rows with missing values
            df_1min.dropna(inplace=True)
            # df_1d.dropna(inplace=True)

            # Calculate the 20-period EMA
            df_1min['EMA10'] = df_1min['c'].ewm(span=10, adjust=False).mean()

            df_1min = df_1min[df_1min.index >= startEpoch]

            # # Determine crossover signals
            # df_1min["EMADown"] = np.where((df_1min["EMA10"] < df_1min["EMA10"].shift(1)), 1, 0)
            # df_1min["EMAUp"] = np.where((df_1min["EMA10"] > df_1min["EMA10"].shift(1)), 1, 0)

            # Add 33360 to the index to match the timestamp
            # df_1d.index = df_1d.index + 33360
            # df_1d.ti = df_1d.ti + 33360

            # df_1d = df_1d[df_1d.index >= ((startEpoch-86340)-(86400*5))]

            stock_1min_data[stock] = df_1min
            # stock_1d_data[stock] = df_1d
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
                "stockcount": None
            }


            df_1min.to_csv(
                f"{self.fileDir['backtestResultsCandleData']}{stock}_1Min.csv")
            # df_1d.to_csv(
            #     f"{self.fileDir['backtestResultsCandleData']}{stock}_1d.csv"
            # )
        


        lastIndexTimeData = [0, 0]
        amountPerTrade = 100000
        TradeLimit = 0
        high_list = []
        low_list = []
        High= None
        Low = None
        Range = None
        New_iteration = False
        df_GL = pd.read_csv("/root/Lakshay_Algos/stocksList/Worst_Indices_Pair.csv")

        df_GL['Datetime'] = pd.to_datetime(df_GL['Datetime'])
        df_GL.set_index(df_GL['Datetime'].dt.date, inplace=True)

        df_Indices = pd.read_csv("/root/Lakshay_Algos/stocksList/All_Indexes_Stocks - Sheet1.csv")
        df_Indices.set_index('Index_Name', inplace=True)


        # At the start of your run() method
        # daily_folder = os.path.join(self.fileDir['backtestResultsStrategyUid'], "GL.ratio_daily")
        # os.makedirs(daily_folder, exist_ok=True)
        

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 
            New_iteration = True

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)
            Today_Date = self.humanTime.date()
            previous_date = Today_Date - timedelta(days=1)

            if previous_date not in df_GL.index:
                 #check if previoud day exists in 1d data
                while previous_date not in df_GL.index:
                    previous_date = previous_date - timedelta(days=1)
            
            Gainer_idx = df_GL.at[previous_date , 'long_leg']
            Loser_idx = df_GL.at[previous_date , 'short_leg']

            top = df_Indices.loc[Gainer_idx].dropna().tolist()
            bottom = df_Indices.loc[Loser_idx].dropna().tolist()
            stock_list = top + bottom


            for stock in stock_list:

                state = stock_state[stock]

                # Then, inside your main loop, use:
                df_1min = stock_1min_data.get(stock)
                # df_1d = stock_1d_data.get(stock)
                
                if df_1min is None:
                    continue

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
                    # state["Static_High"] = None
                    # state["Static_Low"] = None
                    # state["Special_Buy_Trade"] = False
                    # state["Special_Sell_Trade"] = False
                    state["Range"] = None
                    state["SecondTrade"] = False
                    openEpoch = lastIndexTimeData[1]
                    # self.strategyLogger.info(f"{self.humanTime} stocklist: {stock_list}")
                    # stock_merged = []
                    # with open("/root/Lakshay_Algos/stocksList/nifty50.md") as f:
                    #     stock_list = [line.strip() for line in f if line.strip()]
                    if New_iteration:
                        self.strategyLogger.info(f"stock_list: {stock_list}")
                        self.strategyLogger.info(f"top: {top} bottom: {bottom}")
                        self.strategyLogger.info(f"stock_list: {stock_list}")
                        New_iteration = False
                        

                
                # if (self.humanTime.time() > time(9, 16)) and (self.humanTime.time() <= time(9, 21)):
                #     state["high_list"].append(df_1min.at[lastIndexTimeData[1], "h"])
                #     state["low_list"].append(df_1min.at[lastIndexTimeData[1], "l"])

                # if (self.humanTime.time() >= time(9, 21)) and state["Range"] is None:
                #     state["High"] = max(state["high_list"])
                #     state["Low"] = min(state["low_list"])
                #     state["Range"] = state["High"]-state["Low"]
                #     if state["Range"] < 0.002 * (df_1min.at[lastIndexTimeData[1], "o"]):
                #         state["Range"] = 0.002 * (df_1min.at[lastIndexTimeData[1], "o"])
                #         self.strategyLogger.info(f"{self.humanTime} {stock} ATR Range too low, setting to 0.2% of open price: {state['Range']}")
                #     self.strategyLogger.info(f"{self.humanTime} {stock} Range: {state['Range']} High: {state['High']} Low: {state['Low']}")
                        

                # Update current price for open positions
                if not self.openPnl.empty:
                    for index, row in self.openPnl.iterrows():
                        if lastIndexTimeData[1] in df_1min.index:
                            symSide = row["Symbol"]
                            if symSide == stock:
                                self.openPnl.at[index,"CurrentPrice"] = df_1min.at[lastIndexTimeData[1], "c"]

                # Calculate and update PnL
                self.pnlCalculator()
                

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
                                        


                tradecount = self.openPnl['Symbol'].value_counts()
                state["stockcount"]= tradecount.get(stock, 0)


                # if (self.humanTime.time() == time(15, 21)) and New_iteration:
                #     current_day = self.humanTime.date90
                #     daily_csv_path = os.path.join(daily_folder, f"{current_day}.csv")
                #     df_to_save = pd.DataFrame(GL_ratio_records)
                #     # Append if file exists, else write header
                #     df_to_save.to_csv(
                #         daily_csv_path,
                #         mode='a',
                #         header=not os.path.exists(daily_csv_path),
                #         index=False
                #     )
                #     GL_ratio_records = []
                #     New_iteration = False


                # if (self.humanTime.time() <= time(9, 21)):
                #     continue

                # Check for entry signals and execute orders
                if ((timeData-60) in df_1min.index) and (self.humanTime.time() < time(15, 20)):
                    if (state["stockcount"] ==0):
                        if (stock in bottom):

                            entry_price = df_1min.at[lastIndexTimeData[1], "c"]

                            self.entryOrder(entry_price, stock, (amountPerTrade//entry_price), "SELL")

                    
                        if (stock in top):

                            entry_price = df_1min.at[lastIndexTimeData[1], "c"]

                            self.entryOrder(entry_price, stock, (amountPerTrade//entry_price), "BUY")


        # Calculate final PnL and combine CSVs
        self.pnlCalculator()
        self.combinePnlCsv()

        # gl_ratio_df = pd.DataFrame(GL_ratio_records)
        # gl_ratio_df.to_csv(f"{self.fileDir['backtestResultsStrategyUid']}GL_ratio_daily.csv", index=False)

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    # Define Strategy Nomenclature
    devName = "NA"
    strategyName = "rdx"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2025, 7, 16, 9, 15)
    endDate = datetime(2025, 11, 30, 15, 30)

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