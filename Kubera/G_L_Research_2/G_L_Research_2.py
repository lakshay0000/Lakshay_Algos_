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
        with open("/root/Lakshay_Algos/stocksList/fnoStocks173.md") as f:
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

            # Determine crossover signals
            df_1min["EMADown"] = np.where((df_1min["EMA10"] < df_1min["EMA10"].shift(1)), 1, 0)
            df_1min["EMAUp"] = np.where((df_1min["EMA10"] > df_1min["EMA10"].shift(1)), 1, 0)

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

        # Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        # expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        # expiryEpoch= expiryDatetime.timestamp()
        # lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        amountPerTrade = 100000
        New_iteration = False
        GL_ratio_records = []

        # At the start of your run() method
        # daily_folder = os.path.join(self.fileDir['backtestResultsStrategyUid'], "GL.ratio_daily")
        # os.makedirs(daily_folder, exist_ok=True)
        

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 
            New_iteration = True

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

            if lastIndexTimeData[1] not in df.index:
                continue

                #  # Log relevant information
                # if lastIndexTimeData[1] in df.index:
                #     self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")

            if (self.humanTime.time() == time(9, 16)):
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
                # with open("/root/Lakshay_Algos/stocksList/fnoStocks173.md") as f:
                #     stock_list = [line.strip() for line in f if line.strip()]
                

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


            if (self.humanTime.minute % 5 == 0) and (self.humanTime.time() < time(15, 20)):

                top5, bottom5, pct_changes_sorted, Perc_top5, Perc_bottom5 = self.get_daily_top_bottom_stocks(stock_list, openEpoch, lastIndexTimeData[1], stock_1min_data)


                # Sum and count only positive values for gainers
                gainer_stocks = [stock for stock, pct in zip(top5, Perc_top5) if pct > 0]
                gainer_values = [pct for pct in Perc_top5 if pct > 0]
                Gainer_Sum = sum(gainer_values)
                gainer_count = len(gainer_values)
                Gainer_Avg = Gainer_Sum / gainer_count if gainer_count > 0 else 1  # Avoid division by zero

                # Sum and count only negative values for losers
                loser_stocks = [stock for stock, pct in zip(bottom5, Perc_bottom5) if pct < 0]
                loser_values = [pct for pct in Perc_bottom5 if pct < 0]
                Loser_Sum = sum(loser_values)
                loser_count = len(loser_values)
                Loser_Avg = Loser_Sum / loser_count if loser_count > 0 else 1  # Avoid division by zero
                self.strategyLogger.info(f"{self.humanTime} Gainer Sum: {Gainer_Sum}, Loser Sum: {Loser_Sum}, Gainer Avg: {Gainer_Avg}, Loser Avg: {Loser_Avg}")
                self.strategyLogger.info(f"Gainer Stocks: {gainer_stocks}, Loser Stocks: {loser_stocks}")

                selected_stocks = gainer_stocks + loser_stocks

                G_L_Ratio = round(abs(Gainer_Avg) / abs(Loser_Avg), 2)
                self.strategyLogger.info(f"{self.humanTime} Gainer to Loser Ratio: {G_L_Ratio}")

                # G_L_ratio in a list of dictionaries during your loop
                GL_ratio_records.append({
                    "Datetime": self.humanTime,
                    "G_L_ratio": G_L_Ratio
                })

                self.strategyLogger.info(f"{self.humanTime} Gainers: {top5}, Losers: {bottom5}")
                self.strategyLogger.info(f"{self.humanTime} Gainers %: {Perc_top5}, Losers %: {Perc_bottom5}")
                self.strategyLogger.info(f"{self.humanTime} Top Merged: {top_merged}, Bottom Merged: {bottom_merged}")



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


            # if (self.humanTime.time() < time(9, 45)):
            #     continue

            # Check for entry signals and execute orders
            # if ((timeData-60) in df_1min.index) and (self.humanTime.time() < time(15, 20)):

            #     if (stock in top_merged) and (state["stockcount"] ==0):
            #         if df_1min.at[lastIndexTimeData[1], "EMADown"] == 1:

            #             entry_price = df_1min.at[lastIndexTimeData[1], "c"]

            #             self.entryOrder(entry_price, stock, (amountPerTrade//entry_price), "SELL")

            
            #     if (stock in bottom_merged) and (state["stockcount"] ==0):
            #         if df_1min.at[lastIndexTimeData[1], "EMAUp"] == 1:

            #             entry_price = df_1min.at[lastIndexTimeData[1], "c"]

            #             self.entryOrder(entry_price, stock, (amountPerTrade//entry_price), "BUY")


        # Calculate final PnL and combine CSVs
        self.pnlCalculator()
        self.combinePnlCsv()

        gl_ratio_df = pd.DataFrame(GL_ratio_records)
        gl_ratio_df.to_csv(f"{self.fileDir['backtestResultsStrategyUid']}GL_ratio_daily.csv", index=False)

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