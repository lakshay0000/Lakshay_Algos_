import numpy as np
import talib as ta
import pandas_ta as taa
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getFnoBacktestData

# sys.path.insert(1, '/root/backtestTools')


# Define a class algoLogic that inherits from optOverNightAlgoLogic
class algoLogic(optOverNightAlgoLogic):

        # Define a method to get current expiry epoch
    def getCurrentExpiryEpoch(self, date, baseSym):
        # Fetch expiry data for current and next expiry
        expiryData = getExpiryData(date, baseSym)

        # Select appropriate expiry based on the current date
        expiry = expiryData["CurrentExpiry"]

        # Set expiry time to 15:20 and convert to epoch
        expiryDatetime = datetime.strptime(expiry, "%d%b%y")
        expiryDatetime = expiryDatetime.replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()

        return expiryEpoch


    # Define a method to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):

        # Add necessary columns to the DataFrame
        col = ["Expiry"]
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

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")


        lastIndexTimeData = [0, 0]
        flag1=0
        expiryEpoch = self.getCurrentExpiryEpoch(startEpoch, baseSym)
        expiry= datetime.fromtimestamp(expiryEpoch)
        Atmsum = 0
        nearest_premium=0
        flag2=0
        callstrike=0
        putstrike=0
        callpos=0
        putpos=0
        callpoh=0
        putpoh=0
        StraddelStop=0
        pnnl = []


        for timeData in df.index:
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)

            self.timeData = timeData
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

           #skip times period other than trading hours than ()
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue
            # Strategy Specific Trading Time
            # if (self.humanTime.time() < time(9, 20)) | (self.humanTime.time() > time(15, 25)):
            #     continue

            prmtb=[]
            prmtbP=[]
            stikec=[]
            strikep=[]
            prmtb1=[]
            prmtbP1=[]
            callstrkc2=[]
            putstrkp2=[]


            # Update current price for open positions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)
            
             # Log relevant information
            if lastIndexTimeData[1] in df.index:
                self.strategyLogger.info(
                    f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")



            # Calculate and update PnL
            self.pnlCalculator()

            if self.humanTime.date() > expiry.date() :
                expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                expiry= datetime.fromtimestamp(expiryEpoch)


            if lastIndexTimeData[1] in df.index:
                if (self.timeData >= expiryEpoch) and (flag1==0):
                    flag1=1
                    # print(flag1)

            if self.openPnl.shape[0] == 4 and StraddelStop==0:
                sell_positions = self.openPnl[self.openPnl["PositionStatus"] == -1]
                # Separate CE and PE based on the symbol
                ce_positions = sell_positions[sell_positions["Symbol"].str.contains("CE")]
                pe_positions = sell_positions[sell_positions["Symbol"].str.contains("PE")]
                # self.strategyLogger.info(f"ce_positions:{ce_positions}\tpe_positions:{pe_positions}")   
                

                # Calculate CE and PE total values
                ce_value = ce_positions["CurrentPrice"].iloc[0]
                pe_value = pe_positions["CurrentPrice"].iloc[0]
                # self.strategyLogger.info(f"ce_value:{ce_value}\tpe_value:{pe_value}")               

                # columns_to_log = ["EntryTime", "Symbol", "EntryPrice", "PositionStatus"]
                # self.strategyLogger.info(f"{self.openPnl[columns_to_log]}")

                ratio = ce_value / pe_value
                self.strategyLogger.info(f"ratio:{ratio}\tce_value:{ce_value}\tpe_value:{pe_value}")


            if (lastIndexTimeData[1] in df.index) and (flag1==1) and (self.humanTime.time()== time(9, 30)):
                Atmcallsym1 = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=0)
                try:
                    data = self.fetchAndCacheFnoHistData(
                        Atmcallsym1, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)


                AtmputSym1 = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=0)
                try:
                    data1 = self.fetchAndCacheFnoHistData(
                        AtmputSym1, lastIndexTimeData[1])
                except Exception as e:
                    self.strategyLogger.info(e)
                
                Atmsum = data["c"] + data1["c"]
                otmtrade= Atmsum/6
                hedgetrade= Atmsum/2

            if (lastIndexTimeData[1] in df.index) and (flag1==1) and (self.humanTime.time()== time(9, 30)):
                
                for i in range(1,21):
                    callSymotm = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=i)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSymotm, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)
                    
                    callstrikeP= callSymotm[len(callSymotm) - 7:len(callSymotm) - 2]
                    callstrikep=float(callstrikeP)

                    prmtb.append(data["c"])   
                    stikec.append(callstrikep)                  


                        # if (otmtrade-5 <= data["c"] <= otmtrade):
                        #     premium = data["c"]
                        #     callsym=callSymotm

                nearest_premium = min(prmtb, key=lambda x: abs(x - otmtrade))
                premium_index = prmtb.index(nearest_premium)
                otmfactorC= premium_index +1

                for i in range(1,21):
                    putSymotm = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=i)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSymotm, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    putstrikeP= putSymotm[len(putSymotm) - 7:len(putSymotm) - 2]
                    putstrikep=float(putstrikeP)

                    prmtbP.append(data["c"])
                    strikep.append(putstrikep)

                        # if (otmtrade-5 <= data["c"] <= otmtrade):
                        #     premium = data["c"]
                        #     callsym=callSymotm

                nearest_premiumP = min(prmtbP, key=lambda x: abs(x - otmtrade))
                premium_indexP = prmtbP.index(nearest_premiumP)   
                otmfactorP= premium_indexP +1

            else:
                nearest_premiumP=0
                nearest_premium=0
                otmfactorC=0
                otmfactorP=0

                
                
            if lastIndexTimeData[1] in df.index:
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tPremiumPrice:{nearest_premiumP}\totmfactor:{otmfactorP}\totmfactor:{otmfactorC}\tflag1:{flag1}\tflag2: {flag2}")

            # Condition for end straddle
            if self.openPnl.shape[0] == 4:
                for index, row in self.openPnl.iterrows():
                    if row['PositionStatus'] == -1:
                        if row['Symbol'].endswith('CE'):
                            ce_index = row['Symbol'][-7:-2]
                            self.strategyLogger.info(f"ce_index: {ce_index}")                            
                        elif row['Symbol'].endswith('PE'):
                            pe_index = row['Symbol'][-7:-2]
                            self.strategyLogger.info(f"pe_index: {pe_index}")
                if int(ce_index)==int(pe_index):
                    # self.strategyLogger.info(f"No further entries to be taken")
                    StraddelStop= 1

            open_sum= self.openPnl['Pnl'].sum()
            pnnl_sum = sum(pnnl) 
            self.strategyLogger.info(f"pnnl_sum:{pnnl_sum}")


            if (open_sum + pnnl_sum) >= 4500:
                StraddelStop=1
                for index, row in self.openPnl.iterrows():
                    self.exitOrder(index, "MaxLoss")

            if self.openPnl.empty:
                pnnl = []

            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty and self.openPnl.shape[0] == 4:
                for index, row in self.openPnl.iterrows():
                    sym = row["Symbol"][-2:]

                    if self.timeData >= row["Expiry"]:
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)  
                        if StraddelStop==1:
                            StraddelStop=0

                    elif ratio and ratio>2 and StraddelStop==0:
                        if sym == "PE":
                            exitType = "Half Position exit"
                            pnl = row["Pnl"] 
                            pnnl.append(pnl)
                            self.exitOrder(index, exitType)
                            putpos=1

                        if sym=="CE":
                            if row["PositionStatus"]==-1:
                                data5= row["CurrentPrice"]
                                callsym1= int(row['Symbol'][-7:-2])
                                self.strategyLogger.info(f"putpos: {putpos}\tdata5: {data5}")

                    elif ratio and ratio<0.5 and StraddelStop==0:
                        if sym == "CE":
                            exitType = "Half Position exit"
                            pnl = row["Pnl"] 
                            pnnl.append(pnl)
                            self.exitOrder(index, exitType) 
                            callpos=1

                     
                        if sym=="PE":
                            if row["PositionStatus"]==-1:
                                data6= row["CurrentPrice"]
                                putsym1= int(row['Symbol'][-7:-2])
                                self.strategyLogger.info(f"callpos: {callpos}\tdata6: {data6} ")


            if callpos==1:
                for i in range(0,21):
                    callSymotm = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=i)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSymotm, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    callstrkc= callSymotm[len(callSymotm) - 7:len(callSymotm) - 2]
                    callstrkc1=float(callstrkc)

                    callstrkc2.append(callstrkc1)
                    prmtb1.append(data["c"])   

            if putpos==1:
                for i in range(0,21):
                    putSymotm = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=i)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSymotm, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    putstrkp= putSymotm[len(putSymotm) - 7:len(putSymotm) - 2]
                    putstrkp1=float(putstrkp)

                    putstrkp2.append(putstrkp1)
                    prmtbP1.append(data["c"])
                    
            # Condition for end straddle
            # if self.openPnl.shape[0] == 4:
            #     for index, row in self.openPnl.iterrows():
            #         if row['PositionStatus'] == -1:
            #             if row['Symbol'].endswith('CE'):
            #                 ce_index = row['Symbol'][-7:-2]
            #                 self.strategyLogger.info(f"ce_index: {ce_index}")                            
            #             elif row['Symbol'].endswith('PE'):
            #                 pe_index = row['Symbol'][-7:-2]
            #                 self.strategyLogger.info(f"pe_index: {pe_index}")
            #         if int(ce_index)==int(pe_index):
            #             self.strategyLogger.info(f"No further entries to be taken")
            #             continue


            # Check for entry signals and execute orders
            
            if (lastIndexTimeData[1] in df.index) and (self.humanTime.time() < time(14, 15)): 
                
                if (self.humanTime.time() >= time(9,30)) and (flag1==1):
                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=otmfactorC)
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
                    callstrike= callSym[len(callSym) - 7:len(callSym) - 2]
                    HedgeC =float(callstrike)+hedgetrade

                    self.entryOrder(nearest_premium, callSym, lotSize, "SELL", {"Expiry": expiryEpoch,})

                if (self.humanTime.time() >= time(9, 30)) and (flag1==1):
                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=otmfactorP)
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
                    putstrike= putSym[len(putSym) - 7:len(putSym) - 2]
                    HedgeP =float(putstrike) - hedgetrade

                    self.entryOrder(nearest_premiumP, putSym, lotSize, "SELL", {"Expiry": expiryEpoch,})
                    flag1=0
                    flag2=1

                if (self.humanTime.time() >= time(9,30)) and (flag2==1):
                    Hnearest_premiumC = min(stikec, key=lambda x: abs(x - HedgeC))
                    Hpremium_indexC = stikec.index(Hnearest_premiumC)   
                    HotmfactorC= Hpremium_indexC +1
                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=HotmfactorC)
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
                    self.strategyLogger.info(f"otmforhedge:{HotmfactorC}")

                    try:
                        data3 = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)


                    self.entryOrder(data3["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch,})

                if (self.humanTime.time() >= time(9, 30)) and (flag2==1):
                    Hnearest_premiumC = min(strikep, key=lambda x: abs(x - HedgeP))
                    Hpremium_indexP = strikep.index(Hnearest_premiumC)   
                    HotmfactorP= Hpremium_indexP +1
                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=HotmfactorP)
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
                    self.strategyLogger.info(f"otmforhedge:{HotmfactorP}")

                    try:
                        data4 = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data4["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch,})
                    flag2=0


            if StraddelStop== 0:
                if callpos==1:
                    nearest_premiumH = min(prmtb1, key=lambda x: abs(x - data6))
                    premium_indexH = prmtb1.index(nearest_premiumH)
                    otmfactorH= premium_indexH  
                    callsymC= int(callstrkc2[premium_indexH])

                    if callsymC < putsym1:
                        OTM = (putsym1-callsymC)
                        OTM1 = int(OTM/50)
                        otmfactorH = otmfactorH+OTM1 
                        self.strategyLogger.info(f"OTM1: {OTM1}\totmfactorH: {otmfactorH}")


                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=otmfactorH)
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
                    callstrike= callSym[len(callSym) - 7:len(callSym) - 2]
                    HedgeC =float(callstrike)+hedgetrade
                    self.strategyLogger.info(f"callSym:{callSym}")

                    try:
                        data7 = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)   

                    self.entryOrder(data7["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch,})
                    callpos=0
                    callpoh=1


                if putpos==1:
                    nearest_premiumH2 = min(prmtbP1, key=lambda x: abs(x - data5))
                    premium_indexH2 = prmtbP1.index(nearest_premiumH2)
                    otmfactorH2= premium_indexH2 
                    putsymp= int(putstrkp2[premium_indexH2])

                    if putsymp > callsym1:
                        OTM = (putsymp-callsym1)
                        OTM1= int(OTM/50)
                        otmfactorH2 = otmfactorH2+OTM1 
                        self.strategyLogger.info(f"OTM1: {OTM1}\totmfactorH: {otmfactorH2}")

                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=otmfactorH2)
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
                    putstrike= putSym[len(putSym) - 7:len(putSym) - 2]
                    HedgeP =float(putstrike) - hedgetrade
                    self.strategyLogger.info(f"putSym:{putSym}")

                    try:
                        data8 = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)   

                    self.entryOrder(data8["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch,})
                    putpos=0
                    putpoh=1


                if callpoh==1:
                    Hnearest_premiumC = min(callstrkc2, key=lambda x: abs(x - HedgeC))
                    Hpremium_indexC = callstrkc2.index(Hnearest_premiumC)   
                    HotmfactorC= Hpremium_indexC

                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=HotmfactorC)
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
                    self.strategyLogger.info(f"otmforhedge:{HotmfactorC}")

                    try:
                        data9 = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)


                    self.entryOrder(data9["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch,})
                    callpoh=0

                if putpoh==1:
                    Hnearest_premiumC = min(putstrkp2, key=lambda x: abs(x - HedgeP))
                    Hpremium_indexP = putstrkp2.index(Hnearest_premiumC) 
                    HotmfactorP= Hpremium_indexP 

                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=HotmfactorP)
                    expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)
                    lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
                    self.strategyLogger.info(f"otmforhedge:{HotmfactorP}")

                    try:
                        data10 = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data10["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch,})
                    putpoh=0
                    


        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]



if __name__ == "__main__":
    startTime = datetime.now()

    # Define Strategy Nomenclature
    devName = "NA"
    strategyName = "0vernight3"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2023, 1, 1, 9, 15)
    endDate = datetime(2023, 1, 31, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    # Execute the algorithm
    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    # print("Calculating Daily Pnl")
    # dr = calculateDailyReport(
    #     closedPnl, fileDir, timeFrame=timedelta(minutes=5), mtm=True
    # )

    # limitCapital(closedPnl, fileDir, maxCapitalAmount=1000)

    # generateReportFile(dr, fileDir)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")  