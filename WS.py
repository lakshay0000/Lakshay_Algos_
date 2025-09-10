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

    
    def getCallSym(self, baseSym, expiry, indexPrice,strikeDist, otmFactor=0):
        symWithExpiry = baseSym + expiry

        # Calculate nearest multiple of 500
        remainder = indexPrice % strikeDist
        atm = (indexPrice - remainder if remainder <= (strikeDist/2) else indexPrice - remainder + strikeDist)

        if int(atm + (otmFactor * strikeDist)) == (atm + (otmFactor * strikeDist)):
            callSym = (symWithExpiry + str(int(atm + (otmFactor * strikeDist))) + "CE")
        else:
            callSym = (symWithExpiry + str(float(atm + (otmFactor * strikeDist))) + "CE")

        return callSym

    def getPutSym(self, baseSym, expiry, indexPrice, strikeDist, otmFactor=0):
        symWithExpiry = baseSym + expiry

        remainder = indexPrice % strikeDist
        atm = (indexPrice - remainder if remainder <= (strikeDist/2) else (
            indexPrice - remainder + strikeDist))

        if int(atm - (otmFactor * strikeDist)) == (atm - (otmFactor * strikeDist)):
            putSym = (symWithExpiry +
                      str(int(atm - (otmFactor * strikeDist))) + "PE")
        else:
            putSym = (symWithExpiry +
                      str(float(atm - (otmFactor * strikeDist))) + "PE")

        return putSym


    def runBacktest(self, baseSym, startDate, endDate):
        # Set start and end timestamps for data retrieval
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()


        self.humanTime = startDate
        self.addColumnsToOpenPnlDf(["Expiry"])

        self.strategyLogger.info("TEST")

        try:
            df = getFnoBacktestData(
                self.symMap["NIFTY"], startTimeEpoch-(86400 * 30), endTimeEpoch, "T")
            df_SS = getFnoBacktestData(
                self.symMap["SENSEX"], startTimeEpoch-(86400 * 30), endTimeEpoch, "T")
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
        


        lastIndexTimeData = [0, 0]     
        pe_count = 0
        ce_count = 0    
        close_N = 0
        open_N = 0  
        open_NN = 0
        open_SSS = 0
        close_SS = 0
        open_SS = 0
        put_sell_MT_N = False
        call_sell_MT_N = False
        put_sell_MR_N = False
        call_sell_MR_N = False
        put_sell_MT_SS = False
        call_sell_MT_SS = False
        put_sell_MR_SS = False
        call_sell_MR_SS = False        
        previous_trade_date_MR_SS = 0
        previous_trade_date_MT_SS = 0   
        previous_trade_date_MR_N = 0
        previous_trade_date_MT_N = 0
        diff_N = 0
        diff_SS = 0


        currentExpiryN = getExpiryData(startDate, 'NIFTY')['CurrentExpiry']
        currentExpiryDtN = datetime.strptime(
            currentExpiryN, "%d%b%y").replace(hour=15, minute=20)

        currentExpirySS = getExpiryData(startDate, 'SENSEX')['CurrentExpiry']
        currentExpiryDtSS = datetime.strptime(
            currentExpirySS, "%d%b%y").replace(hour=15, minute=20)            
        

        for timeData in df.index:
            for baseSym in ["NIFTY","SENSEX"]:

                self.timeData = timeData
                self.humanTime = datetime.fromtimestamp(timeData)

                lastIndexTimeData.pop(0)
                lastIndexTimeData.append(timeData-60)

                if (baseSym == "NIFTY"):
                    # Expiry for entry
                    if self.humanTime.date() >= currentExpiryDtN.date():
                        currentExpiryN = getExpiryData(self.humanTime + timedelta(days=1), baseSym)['CurrentExpiry']
                        currentExpiryDtN = datetime.strptime(currentExpiryN, "%d%b%y").replace(hour=15, minute=20)     
                elif baseSym == "SENSEX":
                    # Expiry for entry
                    if self.humanTime.date() >= currentExpiryDtSS.date():
                        currentExpirySS = getExpiryData(self.humanTime + timedelta(days=1), baseSym)['CurrentExpiry']
                        currentExpiryDtSS = datetime.strptime(currentExpirySS, "%d%b%y").replace(hour=15, minute=20)    

    
                

                if (lastIndexTimeData[1] in df.index):
                    self.strategyLogger.info(f"Pehla")
                    pe_count = self.openPnl['Symbol'].str.contains('PE').sum()
                    ce_count = self.openPnl['Symbol'].str.contains('CE').sum()


                if not self.openPnl.empty:
                    for index, row in self.openPnl.iterrows():
                        try:
                            data = self.fetchAndCacheFnoHistData(
                                row['Symbol'], lastIndexTimeData[1], maxCacheSize=1000)
                            self.openPnl.at[index,
                                            "CurrentPrice"] = data['c']
                        except Exception as e:
                            self.strategyLogger.info(
                                f"Datetime: {self.humanTime}\tCouldn't update current price for {row['Symbol']} at {lastIndexTimeData[1]}")
                                        


                if (lastIndexTimeData[1] in df.index):
                    self.strategyLogger.info(f"Dooja")

                # Exit
                if not self.openPnl.empty:
                    for index, row in self.openPnl.iterrows():
                        sym = row["Symbol"]
                        symSide = sym[len(sym) - 2:]
                        if (self.humanTime >= row["Expiry"]):
                            exitType = "Expiry Hit"
                            self.exitOrder(index, exitType)
                        elif (row['CurrentPrice'] <= (0.3*row['EntryPrice'])):
                            exitType = "Target"
                            self.exitOrder(index, exitType)
                        elif (row['CurrentPrice'] >= 1.5*row['EntryPrice']):
                            exitType = "Stoploss Hit"
                            self.exitOrder(index, exitType)
                

                self.pnlCalculator()

                
                if (lastIndexTimeData[1] in df.index):
                    self.strategyLogger.info(f"Teeja")

                #Entry Pre-conditions
                # if not last_open_MR or not last_close_MR:
                #     continue

                if (lastIndexTimeData[1] in df.index) and self.humanTime.time() == time(9,16):
                    open_NN = df.at[lastIndexTimeData[1],'o'] 
                    open_SSS = df.at[lastIndexTimeData[1],'o_from_SS']


                if (lastIndexTimeData[1] in df.index):
                    self.strategyLogger.info(f"bham1")                    

                if  (lastIndexTimeData[1] in df.index):
                    self.strategyLogger.info(f"{self.humanTime}")

                if  (lastIndexTimeData[1] in df.index) and self.humanTime.time() == time(15,19):
                    open_N = open_NN
                    open_SS = open_SSS
                    close_N = df.at[timeData,'c'] 
                    close_SS = df.at[timeData,'c_from_SS']


                if (lastIndexTimeData[1] in df.index):
                    self.strategyLogger.info(f"bham2")                    
                
                if (lastIndexTimeData[1] in df.index) and close_N != 0 and close_SS != 0 and open_N != 0 and open_SS != 0:
                    diff_N = round((100*(close_N - open_N)/open_N),6) 
                    diff_SS = round((100*(close_SS - open_SS )/open_SS),6)
                    put_sell_MT_N = False
                    call_sell_MT_N = False
                    put_sell_MR_N = False
                    call_sell_MR_N = False
                    put_sell_MT_SS = False
                    call_sell_MT_SS = False
                    put_sell_MR_SS = False
                    call_sell_MR_SS = False                 
                    #comparing

                if (lastIndexTimeData[1] in df.index):
                    self.strategyLogger.info(f"bham3")

                    #Green Candle with nifty more/less than sensex
                    if (diff_N > diff_SS) and (diff_SS > 0):
                        put_sell_MT_N = True
                        call_sell_MR_SS = True

                                            
                    elif (diff_SS > diff_N) and (diff_N > 0):
                        call_sell_MR_N = True
                        put_sell_MT_SS = True                
                    
                        
                    #Red Candle with nifty more/less than sensex
                    elif (diff_N < diff_SS) and (diff_SS<0):
                        call_sell_MT_N = True
                        put_sell_MR_SS = True

                    elif (diff_N > diff_SS) and (diff_N<0):
                        put_sell_MR_N = True
                        call_sell_MT_SS = True

                    #different Candle
                    elif (diff_N<0 and diff_SS>0) or (diff_N>0 and diff_SS<0):
                        if (diff_N<0 and diff_SS>0):
                            put_sell_MR_N = True
                            call_sell_MT_N = True
                            put_sell_MT_SS = True
                            call_sell_MR_SS = True
                        elif (diff_N>0 and diff_SS<0):
                            put_sell_MR_SS = True
                            call_sell_MT_SS = True
                            put_sell_MT_N = True
                            call_sell_MR_N = True

                
                if (lastIndexTimeData[1] in df.index):
                    self.strategyLogger.info(f"Chautha")
                    self.strategyLogger.info(f"open_N:{open_N}\topen_SS:{open_SS}\tclose_N:{close_N}\tclose_SS:{close_SS}\tdiff_N:{diff_N}\tdiff_SS:{diff_SS}")


                    
                if (lastIndexTimeData[1] in df.index) and ((time(9,16) < self.humanTime.time() < time(9, 21)) or (time(15, 19) < self.humanTime.time() < time(15, 25))):
                    if not diff_N or not diff_SS:
                        continue
                    self.strategyLogger.info(f"open_N:{open_N}\topen_SS:{open_SS}\tclose_N:{close_N}\tclose_SS:{close_SS}\tdiff_N:{diff_N}\tdiff_SS:{diff_SS}")
                    self.strategyLogger.info(f"Datetime: {self.humanTime}\tput_sell_MR_N:{put_sell_MR_N}\tcall_sell_MR_N:{call_sell_MR_N}\tput_sell_MT_N:{put_sell_MT_N}\tcall_sell_MT_N:{call_sell_MT_N}\tput_sell_MR_SS:{put_sell_MR_SS}\tcall_sell_MR_SS:{call_sell_MR_SS}\tput_sell_MT_SS:{put_sell_MT_SS}\tcall_sell_MT_SS:{call_sell_MT_SS}")
                    self.strategyLogger.info(f"ce_count:{ce_count}\tpe_count:{pe_count}")
              
                # Entry
                if (lastIndexTimeData[1] in df.index):
                    if baseSym == "NIFTY":
                        if ((time(9,17) < self.humanTime.time() < time(9, 20)) and (previous_trade_date_MR_N != self.humanTime.date()) and (call_sell_MR_N and (ce_count < 4))) or ((time(15, 19) < self.humanTime.time() < time(15,25)) and (previous_trade_date_MT_N != self.humanTime.date()) and (call_sell_MT_N and (ce_count < 4))):                            
                            
                            ceData = None
                            callSym = self.getCallSym(baseSym, currentExpiryN, df.at[lastIndexTimeData[1], 'c'], 50, otmFactor=0)
                            self.strategyLogger.info(f"Checking entry data for {callSym}")
                            try:
                                ceData = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[0], maxCacheSize=100)
                            except Exception as e:
                                ceData = None
                                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {callSym}")
                            

                            # checking if ceData['c] is not none and is more than 1000 or less than 200
                            if ceData is not None:
                                if ((ceData['c'] > 400) or (ceData['c'] < 100)):
                                    i=0
                                    N=0
                                    if ceData['c'] > 400 :
                                        while (ceData is None or ceData['c'] > 400) and N < 5:
                                            i += 1
                                            callSym = self.getCallSym(baseSym, currentExpiryN, df.at[lastIndexTimeData[1], 'c'], 50, otmFactor=i)
                                            self.strategyLogger.info(f"Checking entry data for {callSym}")
                                            try:
                                                ceData = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[0], maxCacheSize=100)
                                            except Exception as e:
                                                ceData = None
                                                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {callSym}")
                                            N += 1
                                    elif ceData['c'] < 100:
                                        while (ceData is None or ceData['c'] < 100) and N < 5:
                                            i -= 1
                                            callSym = self.getCallSym(baseSym, currentExpiryN, df.at[lastIndexTimeData[1], 'c'], 50, otmFactor=i)
                                            self.strategyLogger.info(f"Checking entry data for {callSym}")
                                            try:
                                                ceData = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[0], maxCacheSize=100)
                                            except Exception as e:
                                                ceData = None
                                                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {callSym}")                                    
                                                N += 1                            
                            
                            
                            if ceData is not None:
                                # quantity = math.floor(50_000 / (ceData['c'] * self.lotSizeMap["NIFTY"])) * self.lotSizeMap["NIFTY"]
                                self.entryOrder(ceData['c'], callSym,  75, "SELL", {"Expiry": currentExpiryDtN})
                                if call_sell_MR_N:
                                    previous_trade_date_MR_N = self.humanTime.date()
                                elif call_sell_MT_N:
                                    previous_trade_date_MT_N = self.humanTime.date()
                                self.strategyLogger.info(f"\nDatetime: {self.humanTime}\tCALL SELL ENTRY {callSym} @ {ceData['c']}\n")
                            else:
                                self.strategyLogger.warning(f"\nDatetime: {self.humanTime}\tData not available for {callSym}\n")



                        elif ((time(9,17) < self.humanTime.time() < time(9, 20)) and (previous_trade_date_MR_N != self.humanTime.date()) and (put_sell_MR_N and  (pe_count < 4))) or ((time(15, 19) < self.humanTime.time() < time(15,25)) and (previous_trade_date_MT_N != self.humanTime.date()) and (put_sell_MT_N and  (pe_count < 4))):
                            peData = None
                            putSym = self.getPutSym(
                                baseSym, currentExpiryN, df.at[lastIndexTimeData[1], 'c'], 50 , otmFactor=0)
                            self.strategyLogger.info(f"Checking entry data for {putSym}")
                            try:
                                peData = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[0], maxCacheSize=100)
                            except Exception as e:
                                peData = None
                                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {putSym}")
                            
                            # checking if peData['c] is not none and is more than 1000 or less than 200
                            if peData is not None:
                                self.strategyLogger.info(f" Checking peData  {peData}")
                                if ((peData['c'] > 400) or (peData['c'] < 100)):
                                    self.strategyLogger.info(f"hi")
                                    i=0 
                                    N=0
                                    if peData['c'] > 400 :
                                        while (peData is None or peData['c'] > 400) and N < 5:
                                            i += 1
                                            putSym = self.getPutSym ( baseSym, currentExpiryN, df.at[lastIndexTimeData[1], 'c'], 50, otmFactor=i)
                                            self.strategyLogger.info(f"Checking entry data for {putSym}")
                                            try:
                                                peData = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[0], maxCacheSize=100)
                                            except Exception as e:
                                                peData = None
                                                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {putSym}")
                                                N+=1
                                    
                                    elif peData['c'] < 100  :
                                        while (peData is None or (peData['c'] < 100) ) and N < 5:
                                            i -= 1
                                            putSym = self.getPutSym ( baseSym, currentExpiryN, df.at[lastIndexTimeData[1], 'c'], 50, otmFactor=i)
                                            self.strategyLogger.info(f"Checking entry data for {putSym}")
                                            try:
                                                peData = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[0], maxCacheSize=100)
                                            except Exception as e:
                                                peData = None
                                                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {putSym}")
                                                N+=1
                                    
                            
                            
                            if peData is not None:
                                # quantity = math.floor(50_000 / (peData['c'] * self.lotSizeMap[baseSym])) * self.lotSizeMap[baseSym]
                                self.entryOrder(peData['c'], putSym,  75 , "SELL", {"Expiry": currentExpiryDtN})
                                if put_sell_MR_N:
                                    previous_trade_date_MR_N = self.humanTime.date()    
                                elif put_sell_MT_N:
                                    previous_trade_date_MT_N = self.humanTime.date()                    
                                self.strategyLogger.info(f"\nDatetime: {self.humanTime}\tPUT SELL ENTRY {putSym} @ {peData['c']}\n")
                            else:
                                self.strategyLogger.warning(f"\nDatetime: {self.humanTime}\tData not available for {putSym}\n")

                    # for sensex
                    elif baseSym == "SENSEX":
                        if ((time(9,17) < self.humanTime.time() < time(9, 20)) and (previous_trade_date_MR_SS != self.humanTime.date()) and (call_sell_MR_SS and (ce_count < 4))) or ((time(15, 19) < self.humanTime.time() < time(15,25)) and (previous_trade_date_MT_SS != self.humanTime.date()) and (call_sell_MT_SS and (ce_count < 4))):                            
                            
                            ceData = None
                            callSym = self.getCallSym(baseSym, currentExpirySS, df.at[lastIndexTimeData[1], 'c_from_SS'], 500, otmFactor=0)
                            self.strategyLogger.info(f"Checking entry data for {callSym}")
                            try:
                                ceData = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[0], maxCacheSize=100)
                            except Exception as e:
                                ceData = None
                                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {callSym}")
                            
                            # checking if ceData['c] is not none and is more than 1000 or less than 200
                            if ceData is not None:
                                if ((ceData['c'] > 1000) or (ceData['c'] < 200)):
                                    i=0
                                    N=0
                                    if ceData['c'] > 1000 :
                                        while (ceData is None or ceData['c'] > 1000) and N < 5:
                                            i += 1
                                            callSym = self.getCallSym(baseSym, currentExpirySS, df.at[lastIndexTimeData[1], 'c_from_SS'], 500, otmFactor=i)
                                            self.strategyLogger.info(f"Checking entry data for {callSym}")
                                            try:
                                                ceData = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[0], maxCacheSize=100)
                                            except Exception as e:
                                                ceData = None
                                                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {callSym}")
                                            N += 1
                                            
                                    elif ceData['c'] < 200:
                                        while (ceData is None or ceData['c'] < 200) and N < 5:
                                            i -= 1
                                            callSym = self.getCallSym(baseSym, currentExpirySS, df.at[lastIndexTimeData[1], 'c_from_SS'], 500, otmFactor=i)
                                            self.strategyLogger.info(f"Checking entry data for {callSym}")
                                            try:
                                                ceData = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[0], maxCacheSize=100)
                                            except Exception as e:
                                                ceData = None
                                                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {callSym}")                                    
                                                N += 1
                                                                
                            
                            if ceData is not None:
                                # quantity = math.floor(50_000 / (ceData['c'] * self.lotSizeMap["NIFTY"])) * self.lotSizeMap["NIFTY"]
                                self.entryOrder(ceData['c'], callSym,  20, "SELL", {"Expiry": currentExpiryDtSS})
                                if call_sell_MR_SS:
                                    previous_trade_date_MR_SS = self.humanTime.date()
                                elif call_sell_MT_SS:
                                    previous_trade_date_MT_SS = self.humanTime.date()
                                self.strategyLogger.info(f"\nDatetime: {self.humanTime}\tCALL SELL ENTRY {callSym} @ {ceData['c']}\n")
                            else:
                                self.strategyLogger.warning(f"\nDatetime: {self.humanTime}\tData not available for {callSym}\n")



                        elif ((time(9,17) < self.humanTime.time() < time(9, 20)) and (previous_trade_date_MR_SS != self.humanTime.date()) and (put_sell_MR_SS and  (pe_count < 4))) or ((time(15, 19) < self.humanTime.time() < time(15,25)) and (previous_trade_date_MT_SS != self.humanTime.date()) and (put_sell_MT_SS and  (pe_count < 4))):
                            peData = None
                            putSym = self.getPutSym(
                                baseSym, currentExpirySS, df.at[lastIndexTimeData[1], 'c_from_SS'], 500, otmFactor=0)
                            self.strategyLogger.info(f"Checking entry data for {putSym}")
                            try:
                                peData = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[0], maxCacheSize=100)
                            except Exception as e:
                                peData = None
                                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {putSym}")
                            
                            # checking if peData['c] is not none and is more than 800 or less than 200
                            if peData is not None:
                                self.strategyLogger.info(f" Checking peData  {peData}")
                                if ((peData['c'] > 1000) or (peData['c'] < 200)):
                                    self.strategyLogger.info(f"hi")
                                    i=0 
                                    N=0
                                    if peData['c'] > 1000 :
                                        while (peData is None or peData['c'] > 1000) and N < 5:
                                            i += 1
                                            putSym = self.getPutSym(
                                                baseSym, currentExpirySS, df.at[lastIndexTimeData[1], 'c_from_SS'], 500, otmFactor=i)
                                            self.strategyLogger.info(f"Checking entry data for {putSym}")
                                            try:
                                                peData = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[0], maxCacheSize=100)
                                            except Exception as e:
                                                peData = None
                                                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {putSym}")                                    
                                                N+=1
                                    
                                    elif peData['c'] < 200  :
                                        while (peData is None or (peData['c'] < 200) ) and N < 5:
                                            i -= 1
                                            putSym = self.getPutSym(
                                                baseSym, currentExpirySS, df.at[lastIndexTimeData[1], 'c_from_SS'], 500, otmFactor=i)

                                            self.strategyLogger.info(f"Checking entry data for {putSym}")
                                            try:
                                                peData = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[0], maxCacheSize=100)
                                            except Exception as e:
                                                peData = None                                    
                                                self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {putSym}")      
                                                N+=1
                                                        
                            
                            if peData is not None:
                                # quantity = math.floor(50_000 / (peData['c'] * self.lotSizeMap[baseSym])) * self.lotSizeMap[baseSym]
                                self.entryOrder(peData['c'], putSym,  20 , "SELL", {"Expiry": currentExpiryDtSS})
                                if put_sell_MR_SS:
                                    previous_trade_date_MR_SS = self.humanTime.date()    
                                elif put_sell_MT_SS:
                                    previous_trade_date_MT_SS = self.humanTime.date()                    
                                self.strategyLogger.info(f"\nDatetime: {self.humanTime}\tPUT SELL ENTRY {putSym} @ {peData['c']}\n")
                            else:
                                self.strategyLogger.warning(f"\nDatetime: {self.humanTime}\tData not available for {putSym}\n")


        return self.combinePnlCsv()



if __name__ == "__main__":
    startNow = datetime.now()



    # Define Strategy Nomenclature
    devName = "NA"
    strategyName = "Magic_N_SS"
    version = "7050_100-400_200-1000"

    # Define Start date and End date
    startDate = datetime(2024, 1, 1, 9, 15)
    # endDate = datetime(2025, 1, 31, 15, 30)
    # endDate = datetime(2024, 1, 11, 0, 0)
    endDate = datetime.now()

    algoLogicObj = Magic_N_SS_Strategy(devName, strategyName, version)   

    # Copy strategy Code
    sourceFile = os.path.abspath(__file__)
    fileDir = algoLogicObj.getFileDir()
    shutil.copy2(sourceFile, fileDir)

    closedPnl = algoLogicObj.runBacktest(['NIFTY','SENSEX'], startDate,
                                         endDate)  

    # Generate metric report based on backtest results
    print("Starting post processing calculation...")

    dailyReport = calculateDailyReport(
        closedPnl, fileDir, timeFrame=timedelta(minutes=15), mtm=True, fno=True)

    # limitCapital(closedPnl, fileDir, maxCapitalAmount=100000)

    generateReportFile(dailyReport, fileDir)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")
