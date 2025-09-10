import threading
import talib
import pandas_ta as taa
import logging
import numpy as np
import multiprocessing
from termcolor import colored, cprint
from datetime import datetime, time
from backtestTools.util import setup_logger
from backtestTools.histData import getEquityHistData,connectToMongo
from backtestTools.histData import getEquityBacktestData
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from datetime import datetime, timedelta
from backtestTools.util import createPortfolio, calculateDailyReport, limitCapital, generateReportFile


class algoLogic(baseAlgoLogic):
    def runBacktest(stockAlgoLogic, portfolio, startDate, endDate):
        if stockAlgoLogic.strategyName != "EquityOvernight":
            raise Exception("Strategy Name Mismatch")

        # Calculate total number of backtests
        total_backtests = sum(len(batch) for batch in portfolio)
        completed_backtests = 0
        cprint(
            f"Backtesting: {stockAlgoLogic.strategyName} UID: {stockAlgoLogic.fileDirUid}", "green")
        print(colored("Backtesting 0% complete.", "light_yellow"), end="\r")

        for batch in portfolio:
            processes = []
            for stock in batch:
                p = multiprocessing.Process(
                    target=stockAlgoLogic.backtest, args=(stock, startDate, endDate))
                p.start()
                processes.append(p)

            # Wait for all processes to finish
            for p in processes:
                p.join()
                completed_backtests += 1
                percent_done = (completed_backtests / total_backtests) * 100
                print(colored(f"Backtesting {percent_done:.2f}% complete.", "light_yellow"), end=(
                    "\r" if percent_done != 100 else "\n"))

        return stockAlgoLogic.fileDir["backtestResultsStrategyUid"], stockAlgoLogic.combinePnlCsv()

    def backtest(stockAlgoLogic, stockName, startDate, endDate):
        conn = connectToMongo()

        # Set start and end timestamps for data retrieval
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()

        stockAlgoLogic = equityOverNightAlgoLogic(stockName, stockAlgoLogic.fileDir)

        # logger = setup_logger("strategyLogger",f"{stockAlgoLogic.fileDir['backtestResultsStrategyLogs']}{stockName}_log.log",)
        # logger.propagate = False

        try:
            # Subtracting 2592000 to subtract 90 days from startTimeEpoch
            df = getEquityBacktestData(
                stockName, startTimeEpoch, endTimeEpoch, "1Min",conn=conn)
            df_1d = getEquityBacktestData(stockName, startTimeEpoch-(86400*50), endTimeEpoch, "1D")
        except Exception as e:
        # Log an exception if data retrieval fails
            stockAlgoLogic.strategyLogger.info(
                f"Data not found for {stockName} in range {startDate} to {endDate}")
            raise Exception(e)   
        
        try:
            df.dropna(inplace=True)
            df_1d.dropna(inplace=True)
        except:
            stockAlgoLogic.strategyLogger.info(f"Data not found for {stockName}")
            return
        
        # Add 33360 to the index to match the timestamp
        df_1d.index = df_1d.index + 33360
        df_1d.ti = df_1d.ti + 33360

        df_1d = df_1d[df_1d.index >= startTimeEpoch-86340]


        df.to_csv(
            f"{stockAlgoLogic.fileDir['backtestResultsCandleData']}{stockName}_1Min.csv")
        df_1d.to_csv(
            f"{stockAlgoLogic.fileDir['backtestResultsCandleData']}{stockName}_1D.csv")

        # Strategy Parameters
        lastIndexTimeData = [0, 0]
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

            stockAlgoLogic.timeData = timeData
            stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)
            print(stockAlgoLogic.humanTime)


            # Skip time periods outside trading hours
            if (stockAlgoLogic.humanTime.time() < time(9, 16)) | (stockAlgoLogic.humanTime.time() > time(15, 30)):
                continue

            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)

            # Strategy Specific Trading Time
            if (stockAlgoLogic.humanTime.time() < time(9, 16)) | (stockAlgoLogic.humanTime.time() > time(15, 25)):
                continue

            if lastIndexTimeData[1] not in df.index:
                continue

            #  # Log relevant information
            # if lastIndexTimeData[1] in df.index:
            #     stockAlgoLogic.strategyLogger.info(f"Datetime: {stockAlgoLogic.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")

            if (stockAlgoLogic.humanTime.time() == time(9, 16)):
                m_upper = None
                m_lower = None
                i=0
                k=0
                j=0
                high_list = []
                low_list = []


            # Update current price for open positions
            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    if lastIndexTimeData[1] in df.index:
                        stockAlgoLogic.openPnl.at[index,"CurrentPrice"] = df.at[lastIndexTimeData[1], "c"]

            # Calculate and update PnL
            stockAlgoLogic.pnlCalculator()



            #Updating daily index
            prev_day = timeData - 86400
            if timeData in df_1d.index:
                Today_open = df.at[lastIndexTimeData[1], 'o']
                Today_high = df.at[lastIndexTimeData[1], 'h']
                Today_low = df.at[lastIndexTimeData[1], 'l']
                #check if previoud day exists in 1d data
                while prev_day not in df_1d.index:
                    prev_day = prev_day - 86400

            if prev_day in df_1d.index:
                prev_DH = (df_1d.at[prev_day, 'h'])
                prev_DL = (df_1d.at[prev_day, 'l'])  
                stockAlgoLogic.strategyLogger.info(f"{stockAlgoLogic.humanTime} Previous Day High: {prev_DH}, Previous Day Low: {prev_DL}, Today Open: {Today_open}, BarNo: {375 + i}")

            if m_upper is None and m_lower is None:
                m_upper = (Today_high - prev_DH) / (375)
                m_lower = (Today_low - prev_DL) / (375)
                stockAlgoLogic.strategyLogger.info(f"{stockAlgoLogic.humanTime} Slope Upper: {m_upper}, Slope Lower: {m_lower}")  

            if lastIndexTimeData[1] in df.index:
                BarNo = 375 + i + k
                upper_ray = prev_DH + (m_upper * BarNo)
                lower_ray = prev_DL + (m_lower * BarNo) 
                i= i + 1
                stockAlgoLogic.strategyLogger.info(f"{stockAlgoLogic.humanTime} Upper Ray: {upper_ray}, Lower Ray: {lower_ray}, BarNo: {BarNo}")
                high_list.append(df.at[lastIndexTimeData[1], "h"])
                low_list.append(df.at[lastIndexTimeData[1], "l"])

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

                stockAlgoLogic.strategyLogger.info(f"{stockAlgoLogic.humanTime} 1 Hour Completed. High List: {Today_high}, Low List: {Today_low}")
                stockAlgoLogic.strategyLogger.info(f"{stockAlgoLogic.humanTime} New Slope Upper: {m_upper}, New Slope Lower: {m_lower}")
            
            if lastIndexTimeData[1] in df.index:
                UnderlyingPrice = df.at[lastIndexTimeData[1], "c"]

            
            # Check for exit conditions and execute exit orders
            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():

                    # symSide = row["Symbol"]
                    # symSide = symSide[len(symSide) - 2:] 

                    if stockAlgoLogic.humanTime.time() >= time(15, 20):
                        exitType = "Time Up"
                        stockAlgoLogic.exitOrder(index, exitType)

                    elif (row["PositionStatus"]==1) and df.at[lastIndexTimeData[1], "c"] < lower_ray:
                        exitType = "Lower Ray Hit"
                        stockAlgoLogic.exitOrder(index, exitType)

                        entry_price = df.at[lastIndexTimeData[1], "c"]

                        stockAlgoLogic.entryOrder(entry_price, stockName, (amountPerTrade//entry_price), "SELL")

                        if stockAlgoLogic.humanTime.time() < time(10, 15):
                            Today_high = max(high_list)
                            high_index = high_list.index(Today_high)
                            m_upper = (Today_high - prev_DH) / (375+high_index)


                    elif (row["PositionStatus"]==-1) and df.at[lastIndexTimeData[1], "c"] > upper_ray:
                        exitType = "Upper Ray Hit"
                        stockAlgoLogic.exitOrder(index, exitType)
                        entry_price = df.at[lastIndexTimeData[1], "c"]

                        stockAlgoLogic.entryOrder(entry_price, stockName, (amountPerTrade//entry_price), "BUY")
                        if stockAlgoLogic.humanTime.time() < time(10, 15):
                            Today_low = min(low_list)
                            low_index = low_list.index(Today_low)
                            m_lower = (Today_low - prev_DL) / (375+low_index)


            if ((timeData-60) in df.index) and stockAlgoLogic.openPnl.empty and (stockAlgoLogic.humanTime.time() < time(15, 20)):

                if df.at[lastIndexTimeData[1], "c"] > upper_ray:

                    entry_price = df.at[lastIndexTimeData[1], "c"]

                    stockAlgoLogic.entryOrder(entry_price, stockName, (amountPerTrade//entry_price), "BUY")
                    
                
                if df.at[lastIndexTimeData[1], "c"] < lower_ray:

                    entry_price = df.at[lastIndexTimeData[1], "c"]


                    stockAlgoLogic.entryOrder(entry_price, stockName, (amountPerTrade//entry_price), "SELL")  

                            

            # if ((timeData-300) in df_15min.index) & (stockAlgoLogic.openPnl.empty) & (stockAlgoLogic.humanTime.time() > time(9, 30)):
            #     if (df_15min.at[last5MinIndexTimeData[1], "c"] > breakp) & (df_15min.at[last5MinIndexTimeData[1], "Scross"] == 1):
            #         entry_price = df_15min.at[last15MinIndexTimeData[1], "c"]

            #         stockAlgoLogic.entryOrder(
            #             entry_price, stockName,  (amountPerTrade//entry_price), "BUY")

        stockAlgoLogic.pnlCalculator()




if __name__ == "__main__":
    startNow = datetime.now()

    # Define Strategy Nomenclature
    devName = "NA"
    # Change 'strategyName' from 'rsiDmiIntraday' to 'rsiDmiOvernight' to switch between strategy
    strategyName = "EquityOvernight"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2024, 1, 2, 9, 15)
    endDate = datetime(2024, 12, 31, 15, 30)
    # endDate = datetime.now()

    portfolio = createPortfolio("/root/Lakshay_Algos/stocksList/nifty50.md",10)

    algoLogicObj = algoLogic(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(
        portfolio, startDate, endDate)


    dailyReport = calculateDailyReport(
        closedPnl, fileDir, timeFrame=timedelta(days=1), mtm=True, fno=False)
    # dailyReport = calculateDailyReport(
    #     closedPnl, fileDir, timeFrame=timedelta(days=1), mtm=True)

    # limitCapital(closedPnl, fileDir, maxCapitalAmount=100000)

    # generateReportFile(dailyReport, fileDir)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")