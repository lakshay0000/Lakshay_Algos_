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

    def get_daily_top_bottom_stocks(self, stock_list, date, target_time_epoch):
        """
        Calculates percentage change for each stock between 9:15 and target_time in epoch and date in humantime.date().
        Returns top 5 and bottom 5 stocks by percentage change.
        """
        # Convert target_time_epoch to a time object
        target_time_dt = datetime.fromtimestamp(target_time_epoch)
        target_time = target_time_dt.time()

        pct_changes = []
        for stock in stock_list:
            start_epoch = int(datetime.combine(date, time(9, 15)).timestamp())
            end_epoch = int(datetime.combine(date, target_time).timestamp())
            try:
                df = getEquityBacktestData(stock, start_epoch, end_epoch, "1Min")
                if df is None or df.empty:
                    self.strategyLogger.info(f"No data for {stock} on {date}")
                    continue
                # df['datetime'] = pd.to_datetime(df['datetime'])
                open915 = df[df['datetime'].dt.time == time(9, 15)]
                target_row = df[df['datetime'].dt.time == target_time]
                if open915.empty or target_row.empty:
                    continue
                price915 = open915.iloc[0]['c']
                price_target = target_row.iloc[0]['c']
                pct_change = ((price_target - price915) / price915) * 100
                pct_changes.append((stock, pct_change))
            except Exception as e:
                print(f"Error for {stock}: {e}")
                continue

        # Sort by percentage change
        pct_changes_sorted = sorted(pct_changes, key=lambda x: x[1], reverse=True)
        top5 = [x[0] for x in pct_changes_sorted[:5]]
        bottom5 = [x[0] for x in pct_changes_sorted[-5:]]
        return top5, bottom5, pct_changes_sorted

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
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)



        for stock in stock_list:

            try:
                # Fetch historical data for backtesting
                df_1min = getEquityBacktestData(stock, startEpoch, endEpoch, "1Min")
                df_1d = getEquityBacktestData(stock , startEpoch-(86400*50), endEpoch, "1D")
            except Exception as e:
                # Log an exception if data retrieval fails
                self.strategyLogger.info(
                    f"Data not found for {baseSym} in range {startDate} to {endDate}")
                raise Exception(e)

            # Drop rows with missing values
            df_1min.dropna(inplace=True)
            df_1d.dropna(inplace=True)

            # Add 33360 to the index to match the timestamp
            df_1d.index = df_1d.index + 33360
            df_1d.ti = df_1d.ti + 33360

            df_1d = df_1d[df_1d.index >= startEpoch-86340]


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
        m_upper = None
        m_lower = None
        i=0
        k=0
        j=0
        amountPerTrade = 100000
        high_list = []
        low_list = []
        

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 
            for stock in stock_list:

                try:
                    # Fetch historical data for backtesting
                    df_1min = getEquityBacktestData(stock, startEpoch, endEpoch, "1Min")
                    df_1d = getEquityBacktestData(stock , startEpoch-(86400*50), endEpoch, "1D")
                except Exception as e:
                    # Log an exception if data retrieval fails
                    self.strategyLogger.info(
                        f"Data not found for {baseSym} in range {startDate} to {endDate}")
                    raise Exception(e)

                # Drop rows with missing values
                df_1min.dropna(inplace=True)
                df_1d.dropna(inplace=True)

                # Add 33360 to the index to match the timestamp
                df_1d.index = df_1d.index + 33360
                df_1d.ti = df_1d.ti + 33360

                df_1d = df_1d[df_1d.index >= ((startEpoch-86340)-(86400*5))]

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
                if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                    continue

                if lastIndexTimeData[1] not in df.index:
                    continue

                #  # Log relevant information
                # if lastIndexTimeData[1] in df.index:
                #     self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")

                if (self.humanTime.time() == time(9, 16)):
                    m_upper = None
                    m_lower = None
                    i=0
                    k=0
                    j=0
                    high_list = []
                    low_list = []

                # Update current price for open positions
                if not self.openPnl.empty:
                    for index, row in self.openPnl.iterrows():
                        if lastIndexTimeData[1] in df_1min.index:
                            self.openPnl.at[index,"CurrentPrice"] = df_1min.at[lastIndexTimeData[1], "c"]

                # Calculate and update PnL
                self.pnlCalculator()



                #Updating daily index
                prev_day = timeData - 86400
                if timeData in df_1d.index:
                    Today_open = df_1min.at[lastIndexTimeData[1], 'o']
                    Today_high = df_1min.at[lastIndexTimeData[1], 'h']
                    Today_low = df_1min.at[lastIndexTimeData[1], 'l']
                    #check if previoud day exists in 1d data
                    while prev_day not in df_1d.index:
                        prev_day = prev_day - 86400

                if prev_day in df_1d.index:
                    prev_DH = (df_1d.at[prev_day, 'h'])
                    prev_DL = (df_1d.at[prev_day, 'l'])  
                    self.strategyLogger.info(f"{self.humanTime} Previous Day High: {prev_DH}, Previous Day Low: {prev_DL}, Today Open: {Today_open}, BarNo: {375 + i}")

                if m_upper is None and m_lower is None:
                    m_upper = (Today_high - prev_DH) / (375)
                    m_lower = (Today_low - prev_DL) / (375)
                    self.strategyLogger.info(f"{self.humanTime} Slope Upper: {m_upper}, Slope Lower: {m_lower}")  

                if lastIndexTimeData[1] in df_1min.index:
                    BarNo = 375 + i + k
                    upper_ray = prev_DH + (m_upper * BarNo)
                    lower_ray = prev_DL + (m_lower * BarNo) 
                    i= i + 1
                    self.strategyLogger.info(f"{self.humanTime} Upper Ray: {upper_ray}, Lower Ray: {lower_ray}, BarNo: {BarNo}")
                    high_list.append(df_1min.at[lastIndexTimeData[1], "h"])
                    low_list.append(df_1min.at[lastIndexTimeData[1], "l"])

                if i == 60:
                    m_upper = None
                    m_lower = None
                    i = 0
                    k = k + 60
                    Today_high = max(high_list)
                    high_index = high_list.index(Today_high)
                    Today_low = min(low_list)
                    low_index = low_list.index(Today_low)
                    high_list = []
                    low_list = []
                    m_upper = (Today_high - prev_DH) / (375+j+high_index)
                    m_lower = (Today_low - prev_DL) / (375+j+low_index)
                    j = j + 60

                    self.strategyLogger.info(f"{self.humanTime} 1 Hour Completed. High List: {Today_high}, Low List: {Today_low}")
                    self.strategyLogger.info(f"{self.humanTime} New Slope Upper: {m_upper}, New Slope Lower: {m_lower}")
                

                # Check for exit conditions and execute exit orders
                if not self.openPnl.empty:
                    for index, row in self.openPnl.iterrows():

                        symSide = row["Symbol"]
                        # symSide = symSide[len(symSide) - 2:]      
                            
                        if self.humanTime.time() >= time(15, 20):
                            exitType = "Time Up"
                            self.exitOrder(index, exitType)
                        
                        if symSide == stock:
                            if (row["PositionStatus"]==1) and df_1min.at[lastIndexTimeData[1], "c"] < lower_ray:
                                exitType = "Lower Ray Hit"
                                self.exitOrder(index, exitType)

                                entry_price = df_1min.at[lastIndexTimeData[1], "c"]

                                self.entryOrder(entry_price, baseSym, (amountPerTrade//entry_price), "SELL")

                                if self.humanTime.time() < time(10, 15):
                                    Today_high = max(high_list)
                                    high_index = high_list.index(Today_high)
                                    m_upper = (Today_high - prev_DH) / (375+high_index)


                            elif (row["PositionStatus"]==-1) and df_1min.at[lastIndexTimeData[1], "c"] > upper_ray:
                                exitType = "Upper Ray Hit"
                                self.exitOrder(index, exitType)
                                entry_price = df_1min.at[lastIndexTimeData[1], "c"] 

                                self.entryOrder(entry_price, baseSym, (amountPerTrade//entry_price), "BUY")
                                if self.humanTime.time() < time(10, 15):
                                    Today_low = min(low_list)
                                    low_index = low_list.index(Today_low)
                                    m_lower = (Today_low - prev_DL) / (375+low_index)

                if (self.humanTime.time() < time(9, 17)):
                    continue

                if (self.humanTime.time() > time(9, 16)) and (self.humanTime.time() < time(15, 20)):

                    top5, bottom5, pct_changes_sorted = self.get_daily_top_bottom_stocks(stock_list, self.humanTime.date(), lastIndexTimeData[1])

                    selected_stocks = top5 + bottom5



                # Check for entry signals and execute orders
                if ((timeData-60) in df_1min.index) and self.openPnl.empty and (self.humanTime.time() < time(15, 20)):
                    if stock not in selected_stocks:
                        if df_1min.at[lastIndexTimeData[1], "c"] > upper_ray:

                            entry_price = df_1min.at[lastIndexTimeData[1], "c"]

                            self.entryOrder(entry_price, stock, (amountPerTrade//entry_price), "BUY")
                            

                        if df_1min.at[lastIndexTimeData[1], "c"] < lower_ray:

                            entry_price = df_1min.at[lastIndexTimeData[1], "c"]


                            self.entryOrder(entry_price, stock, (amountPerTrade//entry_price), "SELL")


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
    startDate = datetime(2024, 1, 2, 9, 15)
    endDate = datetime(2024, 1, 5, 15, 30)

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