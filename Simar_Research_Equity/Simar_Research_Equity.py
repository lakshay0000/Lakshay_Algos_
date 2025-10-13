import numpy as np
import talib as ta
import pandas_ta as taa
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getEquityBacktestData

# sys.path.insert(1, '/root/backtestTools')


# Define a class algoLogic that inherits from optOverNightAlgoLogic
class algoLogic(optOverNightAlgoLogic):

    # Define a method to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):

        # Add necessary columns to the DataFrame
        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            # Fetch historical data for backtesting
            df = getEquityBacktestData(indexSym, startEpoch-(86400*50), endEpoch, "1Min")
            # df_1d = getEquityBacktestData(indexSym, startEpoch-(86400*50), endEpoch, "1D")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        # df_1d.dropna(inplace=True)

        # Calculate the 20-period EMA
        df['EMA10'] = df['c'].ewm(span=10, adjust=False).mean()
        df['EMA20'] = df['c'].ewm(span=20, adjust=False).mean()
        df['EMA30'] = df['c'].ewm(span=30, adjust=False).mean()
        df['EMA40'] = df['c'].ewm(span=40, adjust=False).mean()
        df['EMA60'] = df['c'].ewm(span=60, adjust=False).mean()
        df['EMA80'] = df['c'].ewm(span=80, adjust=False).mean()
        df['EMA100'] = df['c'].ewm(span=100, adjust=False).mean()
        df['EMA130'] = df['c'].ewm(span=130, adjust=False).mean()
        df['EMA160'] = df['c'].ewm(span=160, adjust=False).mean()
        df['EMA190'] = df['c'].ewm(span=190, adjust=False).mean()
        df['EMA220'] = df['c'].ewm(span=220, adjust=False).mean()
        df['EMA280'] = df['c'].ewm(span=280, adjust=False).mean()
        df['EMA340'] = df['c'].ewm(span=340, adjust=False).mean()
        df.dropna(inplace=True)

        df = df[df.index >= startEpoch]

        # Determine crossover signals
        # df["EMADown"] = np.where((df["EMA10"] < df["EMA10"].shift(1)), 1, 0)
        # df["EMAUp"] = np.where((df["EMA10"] > df["EMA10"].shift(1)), 1, 0)

        # # Add 33360 to the index to match the timestamp
        # df_1d.index = df_1d.index + 33360
        # df_1d.ti = df_1d.ti + 33360

        # df_1d = df_1d[df_1d.index >= startEpoch-86340]


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        # df_1d.to_csv(
        #     f"{self.fileDir['backtestResultsCandleData']}{indexName}_1d.csv"
        # )
        


        lastIndexTimeData = [0, 0]

        # Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        # expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        # expiryEpoch= expiryDatetime.timestamp()
        # lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        amountPerTrade = 100000
        main_trade = True
        i=0
        list1=['EMA10','EMA20','EMA30','EMA40','EMA60','EMA80','EMA100','EMA130','EMA160','EMA190','EMA220','EMA280','EMA340']
        Ema= None
        TradeLimit = 0



        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

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

            if lastIndexTimeData[1] == 1748835900:
                continue

            if (self.humanTime.time() == time(9, 16)):
                i=0
                Ema=None
                main_trade = True
                TradeLimit = 0

            #  # Log relevant information
            # if lastIndexTimeData[1] in df.index:
            #     self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")

            # if (self.humanTime.time() == time(9, 16)):
            #     m_upper = None
            #     m_lower = None
            #     i=0
            #     k=0
            #     j=0
            #     high_list = []
            #     low_list = []

            # Update current price for open positions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    if lastIndexTimeData[1] in df.index:
                        self.openPnl.at[index,"CurrentPrice"] = df.at[lastIndexTimeData[1], "c"]

            # Calculate and update PnL
            self.pnlCalculator()

            if self.humanTime.time() <= time(9, 30):
                Ema= list1[i]
                if (self.humanTime.minute % 5 == 0):
                    i = i+1
                    Ema= list1[i]
                    self.strategyLogger.info(f"Datetime: {self.humanTime}\tEma: {Ema} i: {i}")

            if self.humanTime.time() > time(9, 30) and self.humanTime.time()<= time(10, 0):
                if (self.humanTime.minute % 10 == 0):
                    i = i+1
                    Ema = list1[i]
                    self.strategyLogger.info(f"Datetime: {self.humanTime}\tEma: {Ema} i: {i}")

            if self.humanTime.time() > time(10, 0) and self.humanTime.time() <= time(11, 0):
                if (self.humanTime.minute % 15 == 0):
                    i = i+1
                    Ema = list1[i]
                    self.strategyLogger.info(f"Datetime: {self.humanTime}\tEma: {Ema} i: {i}")

            if self.humanTime.time() == time(11, 30):
                i=i+1
                Ema = list1[i]
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEma: {Ema} i: {i}")

            if self.humanTime.time() == time(12, 0):
                i=i+1
                Ema = list1[i]
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEma: {Ema} i: {i}")




            #Updating daily index
            # prev_day = timeData - 86400
            # if timeData in df_1d.index:
            #     Today_open = df.at[lastIndexTimeData[1], 'o']
            #     Today_high = df.at[lastIndexTimeData[1], 'h']
            #     Today_low = df.at[lastIndexTimeData[1], 'l']
            #     #check if previoud day exists in 1d data
            #     while prev_day not in df_1d.index:
            #         prev_day = prev_day - 86400

            # if prev_day in df_1d.index:
            #     prev_DH = (df_1d.at[prev_day, 'h'])
            #     prev_DL = (df_1d.at[prev_day, 'l'])  
            #     self.strategyLogger.info(f"{self.humanTime} Previous Day High: {prev_DH}, Previous Day Low: {prev_DL}, Today Open: {Today_open}, BarNo: {375 + i}")

            # if m_upper is None and m_lower is None:
            #     m_upper = (Today_high - prev_DH) / (375)
            #     m_lower = (Today_low - prev_DL) / (375)
            #     self.strategyLogger.info(f"{self.humanTime} Slope Upper: {m_upper}, Slope Lower: {m_lower}")  

            # if lastIndexTimeData[1] in df.index:
            #     BarNo = 375 + i + k
            #     upper_ray = prev_DH + (m_upper * BarNo)
            #     lower_ray = prev_DL + (m_lower * BarNo) 
            #     i= i + 1
            #     self.strategyLogger.info(f"{self.humanTime} Upper Ray: {upper_ray}, Lower Ray: {lower_ray}, BarNo: {BarNo}")
            #     high_list.append(df.at[lastIndexTimeData[1], "h"])
            #     low_list.append(df.at[lastIndexTimeData[1], "l"])

            # if i == 60:
            #     m_upper = None
            #     m_lower = None
            #     i = 0
            #     k = k + 60
            #     Today_high = max(high_list)
            #     high_index = high_list.index(Today_high)
            #     Today_low = min(low_list)
            #     low_index = low_list.index(Today_low)
            #     high_list = []
            #     low_list = []
            #     m_upper = (Today_high - prev_DH) / (375+j+high_index)
            #     m_lower = (Today_low - prev_DL) / (375+j+low_index)
            #     j = j + 60

            #     self.strategyLogger.info(f"{self.humanTime} 1 Hour Completed. High List: {Today_high}, Low List: {Today_low}")
            #     self.strategyLogger.info(f"{self.humanTime} New Slope Upper: {m_upper}, New Slope Lower: {m_lower}")
            

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
                        main_trade = True


                    elif (row["PositionStatus"]==1):
                        if df.at[lastIndexTimeData[0], Ema] > df.at[lastIndexTimeData[1], Ema]:
                            exitType = "Negative Slope change"
                            self.exitOrder(index, exitType)

                            main_trade = True


                    elif (row["PositionStatus"]==-1):
                        if df.at[lastIndexTimeData[0], Ema] < df.at[lastIndexTimeData[1], Ema]:
                            exitType = "Positive Slope change"
                            self.exitOrder(index, exitType)

                            main_trade = True


            # if (self.humanTime.time() < time(9, 30)):
            #     continue
            
            # Check for entry signals and execute orders
            if ((timeData-60) in df.index) and (self.humanTime.time() < time(15, 20)):
                if main_trade and TradeLimit<3:
                    if df.at[lastIndexTimeData[0], Ema] > df.at[lastIndexTimeData[1], Ema]:

                        entry_price = df.at[lastIndexTimeData[1], "c"]

                        self.entryOrder(entry_price, baseSym, (amountPerTrade//entry_price), "SELL")
                        main_trade = False
                        TradeLimit = TradeLimit+1

                    if df.at[lastIndexTimeData[0], Ema] < df.at[lastIndexTimeData[1], Ema]:

                        entry_price = df.at[lastIndexTimeData[1], "c"]

                        self.entryOrder(entry_price, baseSym, (amountPerTrade//entry_price), "BUY")
                        main_trade = False
                        TradeLimit = TradeLimit+1


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
    startDate = datetime(2020, 1, 1, 9, 15)
    endDate = datetime(2025, 8, 30, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "HDFCBANK"
    indexName = "HDFCBANK"

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