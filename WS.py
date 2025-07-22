from datetime import datetime, time, timedelta, date
import math
import multiprocessing
import numpy as np
import talib as ta
import pandas_ta as pta
from termcolor import colored, cprint
from backtestTools.util import setup_logger, createPortfolio, calculateDailyReport, limitCapital, generateReportFile
from backtestTools.expiry import getExpiryData
from backtestTools.histData import getEquityBacktestData, getFnoBacktestData, connectToMongo
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic, optOverNightAlgoLogic
import os
import shutil
import logging
import pandas as pd


class Magic_N_SS_Strategy(optOverNightAlgoLogic):
    def __init__(self, devName, strategyName, version):
        super().__init__(devName, strategyName, version)
        self.symMap = {
            "NIFTY": "NIFTY 50",
            "SENSEX": "SENSEX"}

        self.strikeDistMap = {
            "NIFTY": 50,
            "SENSEX": 100
        }

        self.lotSizeMap = {
            "NIFTY": 75,
            "SENSEX": 20
        }

    def getFileDir(self):
        return self.fileDir["backtestResultsStrategyUid"]

    
    # def getCallSym(self, baseSym, expiry, indexPrice,strikeDist, otmFactor=0):
    #     symWithExpiry = baseSym + expiry

    #     # Calculate nearest multiple of 500
    #     remainder = indexPrice % strikeDist
    #     atm = (indexPrice - remainder if remainder <= (strikeDist/2) else indexPrice - remainder + strikeDist)

    #     if int(atm + (otmFactor * strikeDist)) == (atm + (otmFactor * strikeDist)):
    #         callSym = (symWithExpiry + str(int(atm + (otmFactor * strikeDist))) + "CE")
    #     else:
    #         callSym = (symWithExpiry + str(float(atm + (otmFactor * strikeDist))) + "CE")

    #     return callSym

    # def getPutSym(self, baseSym, expiry, indexPrice, strikeDist, otmFactor=0):
    #     symWithExpiry = baseSym + expiry

    #     remainder = indexPrice % strikeDist
    #     atm = (indexPrice - remainder if remainder <= (strikeDist/2) else (
    #         indexPrice - remainder + strikeDist))

    #     if int(atm - (otmFactor * strikeDist)) == (atm - (otmFactor * strikeDist)):
    #         putSym = (symWithExpiry +
    #                   str(int(atm - (otmFactor * strikeDist))) + "PE")
    #     else:
    #         putSym = (symWithExpiry +
    #                   str(float(atm - (otmFactor * strikeDist))) + "PE")

    #     return putSym


    def runBacktest(self, baseSym, startDate, endDate):
        # Set start and end timestamps for data retrieval
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()


        self.humanTime = startDate
        self.addColumnsToOpenPnlDf(["Expiry"])

        self.strategyLogger.info("TEST")

        try:
            df = getFnoBacktestData(
                self.symMap["NIFTY"], startTimeEpoch-(86400 * 30), endTimeEpoch, "1Min")
            df_SS = getFnoBacktestData(
                self.symMap["SENSEX"], startTimeEpoch-(86400 * 30), endTimeEpoch, "1Min")
        except Exception as e:
            raise Exception(e)

        if df is None:
            self.strategyLogger.info(f"Data not found for {'NIFTY'}")
            return

        df.dropna(inplace=True)
        df = df[df.index >= startTimeEpoch]

        # Filter dataframe from timestamp greater than (30 mins before) start time timestamp
        df = df[df.index > startTimeEpoch - (15*60*2)]

        df.dropna(inplace=True)
        df = df[df.index >= startTimeEpoch]
  
        # Ensure 'ti' exists in both DataFrames and is the key to match
        if 'ti' in df.columns and 'ti' in df_SS.columns:
            # Merge 'c' (close) and 'o' (open) from df_SS into df based on 'ti'
            df = pd.merge(df, df_SS[['ti', 'c', 'o']], on='ti', how='left', suffixes=('', '_SS'))

            # Optionally rename the new columns for clarity or overwrite existing 'c' and 'o'
            df.rename(columns={'c_SS': 'c_from_SS', 'o_SS': 'o_from_SS'}, inplace=True)

        #setting index to ti
        df.set_index('ti', inplace=True)

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{'NIFTY'}_1Min.csv")
        

     


        currentExpiryN = getExpiryData(startDate, 'NIFTY')['CurrentExpiry']
        currentExpiryDtN = datetime.strptime(currentExpiryN, "%d%b%y").replace(hour=15, minute=20)
        expiryEpochN = currentExpiryDtN.timestamp()
        lotSizeN = int(getExpiryData(startDate, 'NIFTY')["LotSize"])

        currentExpirySS = getExpiryData(startDate, 'SENSEX')['CurrentExpiry']
        currentExpiryDtSS = datetime.strptime(currentExpirySS, "%d%b%y").replace(hour=15, minute=20) 
        expiryEpochSS = currentExpiryDtSS.timestamp()
        lotSizeSS = int(getExpiryData(startDate, 'SENSEX')["LotSize"])     

        NIFTY_Straddle_Sell = False
        SENSEX_Straddle_BUY = False
        SENSEX_Straddle_Sell = False
        NIFTY_Straddle_BUY = False  
        lastIndexTimeData = [0, 0]   
        NIFTY_Sell = False
        SENSEX_Sell = False
        NIFTY_Rollover = False
        SENSEX_Rollover = False
        

        for timeData in df.index:

            self.timeData = timeData
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)

            # Expiry for entry
            if self.timeData >= expiryEpochN:
                currentExpiryN = getExpiryData(self.timeData, "NIFTY")['NextExpiry']
                currentExpiryDtN = datetime.strptime(currentExpiryN, "%d%b%y").replace(hour=15, minute=20)   
                expiryEpochN = currentExpiryDtN.timestamp()  

            # Expiry for entry
            if self.timeData >= expiryEpochSS:
                currentExpirySS = getExpiryData(self.timeData,"SENSEX")['NextExpiry']
                currentExpiryDtSS = datetime.strptime(currentExpirySS, "%d%b%y").replace(hour=15, minute=20)    
                expiryEpochSS = currentExpiryDtSS.timestamp()

            

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            row['Symbol'], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data['c']
                    except Exception as e:
                        self.strategyLogger.info(
                            f"Datetime: {self.humanTime}\tCouldn't update current price for {row['Symbol']} at {lastIndexTimeData[1]}")
                        
            if self.openPnl.empty:
                if expiryEpochN < expiryEpochSS:
                    NIFTY_Straddle_Sell = True
                    SENSEX_Straddle_BUY = True
                    SENSEX_Straddle_Sell = False
                    NIFTY_Straddle_BUY = False 
                else:
                    SENSEX_Straddle_Sell = True
                    NIFTY_Straddle_BUY = True
                    NIFTY_Straddle_Sell = False
                    SENSEX_Straddle_BUY = False

            if ((timeData-60) in df.index):
                if (baseSym == "NIFTY"):
                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= currentExpiryN)
                    Current_N_ATMstrike= callSym[len(callSym) - 7:len(callSym) - 2]

                if baseSym == "SENSEX":
                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c_from_SS"],expiry= currentExpirySS)
                    Current_SS_ATMstrike= callSym[len(callSym) - 7:len(callSym) - 2]
            
            if not self.openPnl.empty:
                if NIFTY_Sell:
                    if baseSym == "NIFTY":
                        if (Current_N_ATMstrike != N_ATMstrike):
                            self.strategyLogger.info(
                                f"Datetime: {self.humanTime}\tNIFTY ATM Strike changed from {N_ATMstrike} to {Current_N_ATMstrike}")
                            NIFTY_Rollover = True

                if SENSEX_Sell:
                    if baseSym == "SENSEX":
                        if (Current_SS_ATMstrike != SS_ATMstrike):
                            self.strategyLogger.info(
                                f"Datetime: {self.humanTime}\tSENSEX ATM Strike changed from {SS_ATMstrike} to {Current_SS_ATMstrike}")
                            SENSEX_Rollover = True


            # Exit
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    sym = row["Symbol"]
                    symSide = sym[len(sym) - 2:]

                    if (self.timeData >= row["Expiry"]):
                        exitType = "Expiry Hit"
                        self.exitOrder(index, exitType)
                        NIFTY_Sell = False
                        SENSEX_Sell = False

                    elif NIFTY_Rollover:
                        exitType = "NIFTY Rollover Hit"
                        self.exitOrder(index, exitType)

                    
                    elif SENSEX_Rollover:
                        exitType = "SENSEX Rollover Hit"
                        self.exitOrder(index, exitType)



            self.pnlCalculator()

            
            # Entry
            if ((timeData-60) in df.index) and self.humanTime.time() == time(15, 21):
                if NIFTY_Straddle_Sell:
                    if baseSym == "NIFTY":
                        callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= currentExpiryN)
                        N_ATMstrike= callSym[len(callSym) - 7:len(callSym) - 2]

                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        self.entryOrder(data["c"], callSym, lotSizeN, "SELL", {"Expiry": expiryEpochN})

                        putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= currentExpiryN)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        self.entryOrder(data["c"], putSym, lotSizeN, "SELL", {"Expiry": expiryEpochN})
                        NIFTY_Straddle_Sell = False
                        NIFTY_Sell = True
                        

                if SENSEX_Straddle_BUY:  
                # for sensex
                    if baseSym == "SENSEX":
                        callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c_from_SS"],expiry= currentExpirySS)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        self.entryOrder(data["c"], callSym, lotSizeSS, "BUY", {"Expiry": expiryEpochN})

                        putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c_from_SS"],expiry= currentExpirySS)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        self.entryOrder(data["c"], putSym, lotSizeSS, "BUY", {"Expiry": expiryEpochN})
                        SENSEX_Straddle_BUY = False


                if NIFTY_Straddle_BUY:
                    if baseSym == "NIFTY":
                        callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= currentExpiryN)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        self.entryOrder(data["c"], callSym, lotSizeN, "BUY", {"Expiry": expiryEpochSS})

                        putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= currentExpiryN)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        self.entryOrder(data["c"], putSym, lotSizeN, "BUY", {"Expiry": expiryEpochSS})
                        NIFTY_Straddle_BUY = False


                if SENSEX_Straddle_Sell:    
                # for sensex
                    if baseSym == "SENSEX":
                        callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c_from_SS"],expiry= currentExpirySS)
                        SS_ATMstrike= callSym[len(callSym) - 7:len(callSym) - 2]
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        self.entryOrder(data["c"], callSym, lotSizeSS, "SELL", {"Expiry": expiryEpochSS})

                        putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c_from_SS"],expiry= currentExpirySS)
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        self.entryOrder(data["c"], putSym, lotSizeSS, "SELL", {"Expiry": expiryEpochSS})
                        SENSEX_Straddle_Sell = False
                        SENSEX_Sell = True

            if ((timeData-60) in df.index) and NIFTY_Rollover:
                # NIFTY Rollover
                callSym = self.getCallSym(self.timeData, "NIFTY", df.at[lastIndexTimeData[1], "c"],expiry= currentExpiryN)
                N_ATMstrike= callSym[len(callSym) - 7:len(callSym) - 2]

                try:
                    data = self.fetchAndCacheFnoHistData(
                        callSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)

                self.entryOrder(data["c"], callSym, lotSizeN, "SELL", {"Expiry": expiryEpochN})

                putSym = self.getPutSym(self.timeData, "NIFTY", df.at[lastIndexTimeData[1], "c"],expiry= currentExpiryN)
                try:
                    data = self.fetchAndCacheFnoHistData(
                        putSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)

                self.entryOrder(data["c"], putSym, lotSizeN, "SELL", {"Expiry": expiryEpochN})

                # for sensex
                callSym = self.getCallSym(self.timeData, "SENSEX", df.at[lastIndexTimeData[1], "c_from_SS"],expiry= currentExpirySS)
                try:
                    data = self.fetchAndCacheFnoHistData(
                        callSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)

                self.entryOrder(data["c"], callSym, lotSizeN, "BUY", {"Expiry": expiryEpochN})

                putSym = self.getPutSym(self.timeData, "SENSEX", df.at[lastIndexTimeData[1], "c_from_SS"],expiry= currentExpirySS)
                try:
                    data = self.fetchAndCacheFnoHistData(
                        putSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)

                self.entryOrder(data["c"], putSym, lotSizeN, "BUY", {"Expiry": expiryEpochN})
                NIFTY_Rollover = False


            if ((timeData-60) in df.index) and SENSEX_Rollover:
                # SENSEX Rollover
                callSym = self.getCallSym(self.timeData, "SENSEX", df.at[lastIndexTimeData[1], "c_from_SS"],expiry= currentExpirySS)
                SS_ATMstrike= callSym[len(callSym) - 7:len(callSym) - 2]

                try:
                    data = self.fetchAndCacheFnoHistData(
                        callSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)

                self.entryOrder(data["c"], callSym, lotSizeSS, "SELL", {"Expiry": expiryEpochSS})

                putSym = self.getPutSym(self.timeData, "SENSEX", df.at[lastIndexTimeData[1], "c_from_SS"],expiry= currentExpirySS)
                try:
                    data = self.fetchAndCacheFnoHistData(
                        putSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)

                self.entryOrder(data["c"], putSym, lotSizeSS, "SELL", {"Expiry": expiryEpochSS})

                # for nifty
                callSym = self.getCallSym(self.timeData, "NIFTY", df.at[lastIndexTimeData[1], "c"],expiry= currentExpiryN)
                try:
                    data = self.fetchAndCacheFnoHistData(
                        callSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)

                self.entryOrder(data["c"], callSym, lotSizeN, "BUY", {"Expiry": expiryEpochSS})

                putSym = self.getPutSym(self.timeData, "NIFTY", df.at[lastIndexTimeData[1], "c"],expiry= currentExpiryN)
                try:
                    data = self.fetchAndCacheFnoHistData(
                        putSym, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)

                self.entryOrder(data["c"], putSym, lotSizeN, "BUY", {"Expiry": expiryEpochSS})
                SENSEX_Rollover = False

                        

        return self.combinePnlCsv()



if __name__ == "__main__":
    startNow = datetime.now()



    # Define Strategy Nomenclature
    devName = "NA"
    strategyName = "Magic_N_SS"
    version = "7050_100-400_200-1000"

    # Define Start date and End date
    startDate = datetime(2025, 1, 10, 9, 15)
    # endDate = datetime(2025, 1, 31, 15, 30)
    # endDate = datetime(2024, 1, 11, 0, 0)
    endDate = datetime(2025, 6, 1, 15, 30)

    algoLogicObj = Magic_N_SS_Strategy(devName, strategyName, version)

    # Copy strategy Code
    sourceFile = os.path.abspath(__file__)
    fileDir = algoLogicObj.getFileDir()
    shutil.copy2(sourceFile, fileDir)

    closedPnl = algoLogicObj.runBacktest(['NIFTY','SENSEX'], startDate, endDate)  

    # Generate metric report based on backtest results
    print("Starting post processing calculation...")

    dailyReport = calculateDailyReport(
        closedPnl, fileDir, timeFrame=timedelta(minutes=15), mtm=True, fno=True)

    # limitCapital(closedPnl, fileDir, maxCapitalAmount=100000)

    generateReportFile(dailyReport, fileDir)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")
