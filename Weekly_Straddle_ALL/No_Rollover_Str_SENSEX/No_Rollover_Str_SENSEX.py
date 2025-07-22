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

    
    def OptChain(self, date, symbol, IndexPrice, baseSym):
        prmtb=[]
        if (symbol== "CE"):
            for i in range(0,10):
                callSymotm = self.getCallSym(date, baseSym, IndexPrice, otmFactor=i)         
                try:
                    data = self.fetchAndCacheFnoHistData(
                        callSymotm, date)
                    prmtb.append(data["c"])
                except Exception as e:
                    self.strategyLogger.info(e)
                
                callstrikeP= callSymotm[len(callSymotm) - 7:len(callSymotm) - 2]
                callstrikep=float(callstrikeP)

                # prmtb.append(data["c"])   
                # stike.append(callstrikep)    

        if (symbol== "PE"):
            for i in range(0,10):
                putSymotm = self.getPutSym(date, baseSym, IndexPrice, otmFactor=i)
                try:
                    data = self.fetchAndCacheFnoHistData(
                        putSymotm, date)
                    prmtb.append(data["c"])
                except Exception as e:
                    self.strategyLogger.info(e)

                putstrikeP= putSymotm[len(putSymotm) - 7:len(putSymotm) - 2]
                putstrikep=float(putstrikeP)

                # prmtb.append(data["c"])
                # stike.append(putstrikep)

        return prmtb
        
    def Otmfactor(self, premiumlst, data):
        nearest_premium = min(premiumlst, key=lambda x: abs(x - data))
        premium_index = premiumlst.index(nearest_premium)
        otmfactor= premium_index

        return otmfactor


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
        # df = df[df.index > startTimeEpoch - (15*60*2)]

        df_SS.dropna(inplace=True)
        df_SS = df_SS[df_SS.index >= startTimeEpoch]

  
        # Ensure 'ti' exists in both DataFrames and is the key to match
        if 'ti' in df.columns and 'ti' in df_SS.columns:
            # Merge 'c' (close) and 'o' (open) from df_SS into df based on 'ti'
            df = pd.merge(df, df_SS[['ti', 'c', 'o']], on='ti', how='left', suffixes=('', '_SS'))

            # Optionally rename the new columns for clarity or overwrite existing 'c' and 'o'
            df.rename(columns={'c_SS': 'c_from_SS', 'o_SS': 'o_from_SS'}, inplace=True)

        #setting index to ti
        df.set_index('ti', inplace=True)

        df.dropna(inplace=True)
        df = df[df.index >= startTimeEpoch]

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


        lastIndexTimeData = [0, 0]  
        prmtb = [] 
        

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
                currentExpirySS = getExpiryData(self.timeData, "SENSEX")['NextExpiry']
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

        
                            


            #     if SENSEX_Sell:
            #         if baseSym == "SENSEX":
            #             if (Current_SS_ATMstrike != SS_ATMstrike):
            #                 self.strategyLogger.info(
            #                     f"Datetime: {self.humanTime}\tSENSEX ATM Strike changed from {SS_ATMstrike} to {Current_SS_ATMstrike}")
            #                 SENSEX_Rollover = True


            # Exit
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    sym = row["Symbol"]
                    symSide = sym[len(sym) - 2:]

                    if (self.timeData >= row["Expiry"]):
                        exitType = "Expiry Hit"
                        self.exitOrder(index, exitType)


            self.pnlCalculator()

            
            # Entry
            if ((timeData-60) in df.index):
                # if (self.humanTime.date() == currentExpiryDtN.date()) and self.openPnl.empty:
                #     callSym = self.getCallSym(self.timeData, "NIFTY", df.at[lastIndexTimeData[1], "c"],expiry= currentExpiryN)
                #     try:
                #         data = self.fetchAndCacheFnoHistData(
                #             callSym, lastIndexTimeData[1])
                #     except Exception as e:
                #         self.strategyLogger.info(e)
                    
                #     N_Data_CE = data["c"]
                #     self.entryOrder(data["c"], callSym, lotSizeN, "SELL", {"Expiry": expiryEpochN})

                #     putSym = self.getPutSym(self.timeData, "NIFTY", df.at[lastIndexTimeData[1], "c"],expiry= currentExpiryN)
                #     try:
                #         data = self.fetchAndCacheFnoHistData(
                #             putSym, lastIndexTimeData[1])
                #     except Exception as e:
                #         self.strategyLogger.info(e)

                #     N_Data_PE = data["c"]
                #     self.entryOrder(data["c"], putSym, lotSizeN, "SELL", {"Expiry": expiryEpochN})

                #     N_Straddle_Premium = (N_Data_CE + N_Data_PE)* lotSizeN
                #     SS_Strangle_Premium = (N_Straddle_Premium / lotSizeSS)
  
                # # for sensex
                #     prmtb = self.OptChain(self.timeData, "CE", df.at[lastIndexTimeData[1], "c_from_SS"], "SENSEX")
                #     otmfactor_CE = self.Otmfactor(prmtb, SS_Strangle_Premium)
                #     self.strategyLogger.info(f"prmtb: {prmtb}\tOTM Factor for CE: {otmfactor_CE}\tSS_Strangle_Premium: {SS_Strangle_Premium}")  
                #     callSym = self.getCallSym(self.timeData, "SENSEX", df.at[lastIndexTimeData[1], "c_from_SS"],expiry= currentExpirySS, otmFactor=otmfactor_CE)
                #     try:
                #         data = self.fetchAndCacheFnoHistData(
                #             callSym, lastIndexTimeData[1])
                #     except Exception as e:
                #         self.strategyLogger.info(e)
                    
                #     self.entryOrder(data["c"], callSym, lotSizeSS, "BUY", {"Expiry": expiryEpochN})
                    
                #     prmtb = self.OptChain(self.timeData, "PE", df.at[lastIndexTimeData[1], "c_from_SS"], "SENSEX")
                #     otmfactor_PE = self.Otmfactor(prmtb, SS_Strangle_Premium)
                #     self.strategyLogger.info(f"prmtb: {prmtb}\tOTM Factor for PE: {otmfactor_PE}\tSS_Strangle_Premium: {SS_Strangle_Premium}") 
                #     putSym = self.getPutSym(self.timeData,"SENSEX", df.at[lastIndexTimeData[1], "c_from_SS"],expiry= currentExpirySS, otmFactor= otmfactor_PE)
                #     try:
                #         data = self.fetchAndCacheFnoHistData(
                #             putSym, lastIndexTimeData[1])
                #     except Exception as e:
                #         self.strategyLogger.info(e)

                #     self.entryOrder(data["c"], putSym, lotSizeSS, "BUY", {"Expiry": expiryEpochN})



                if (self.humanTime.date() == currentExpiryDtSS.date()) and self.openPnl.empty:
                    # for sensex
                    callSym = self.getCallSym(self.timeData, "SENSEX", df.at[lastIndexTimeData[1], "c_from_SS"],expiry= currentExpirySS)
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)
                    
                    SS_Data_CE = data["c"]
                    self.entryOrder(data["c"], callSym, lotSizeSS, "SELL", {"Expiry": expiryEpochSS})

                    putSym = self.getPutSym(self.timeData, "SENSEX", df.at[lastIndexTimeData[1], "c_from_SS"],expiry= currentExpirySS)
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    SS_Data_PE = data["c"]
                    self.entryOrder(data["c"], putSym, lotSizeSS, "SELL", {"Expiry": expiryEpochSS})

                    SS_Straddle_Premium = (SS_Data_CE + SS_Data_PE) * lotSizeSS
                    N_Strangle_Premium = (SS_Straddle_Premium / lotSizeN)

                    # for nifty
                    prmtb = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], "NIFTY")
                    otmfactor_CE = self.Otmfactor(prmtb, N_Strangle_Premium)
                    callSym = self.getCallSym(self.timeData, "NIFTY", df.at[lastIndexTimeData[1], "c"],expiry= currentExpiryN, otmFactor=otmfactor_CE)
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)
                    
                    self.entryOrder(data["c"], callSym, lotSizeN, "BUY", {"Expiry": expiryEpochSS})
                    
                    prmtb = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], "NIFTY")
                    otmfactor_PE = self.Otmfactor(prmtb, N_Strangle_Premium)
                    putSym = self.getPutSym(self.timeData, "NIFTY", df.at[lastIndexTimeData[1], "c"],expiry= currentExpiryN, otmFactor= otmfactor_PE)
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSizeN, "BUY", {"Expiry": expiryEpochSS})



        return self.combinePnlCsv()



if __name__ == "__main__":
    startNow = datetime.now()



    # Define Strategy Nomenclature
    devName = "NA"
    strategyName = "Magic_N_SS"
    version = "7050_100-400_200-1000"

    # Define Start date and End date
    startDate = datetime(2024, 1, 10, 9, 15)
    # endDate = datetime(2025, 1, 31, 15, 30)
    # endDate = datetime(2024, 1, 11, 0, 0)
    endDate = datetime(2024, 12, 10, 15, 30)

    algoLogicObj = Magic_N_SS_Strategy(devName, strategyName, version)

    # Copy strategy Code
    # sourceFile = os.path.abspath(__file__)
    # fileDir = algoLogicObj.getFileDir()
    # shutil.copy2(sourceFile, fileDir)

    closedPnl = algoLogicObj.runBacktest(['NIFTY','SENSEX'], startDate, endDate)  

    # Generate metric report based on backtest results
    print("Starting post processing calculation...")

    # dailyReport = calculateDailyReport(
    #     closedPnl, fileDir, timeFrame=timedelta(minutes=15), mtm=True, fno=True)

    # # limitCapital(closedPnl, fileDir, maxCapitalAmount=100000)

    # generateReportFile(dailyReport, fileDir)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")
