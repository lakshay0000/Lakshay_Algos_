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
    
    def OptChain(self, date, symbol, IndexPrice):
        prmtb=[]
        stike=[]
        if (symbol== "CE"):
            for i in range(0,21):
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
                stike.append(callstrikep)    

        if (symbol== "PE"):
            for i in range(0,21):
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
                stike.append(putstrikep)

        return prmtb,stike
        
    def Otmfactor(self, premiumlst, data):
        nearest_premium = min(premiumlst, key=lambda x: abs(x - data))
        premium_index = premiumlst.index(nearest_premium)
        otmfactor= premium_index

        return otmfactor


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
        lotSize = int(getExpiryData(startEpoch, baseSym)["LotSize"])
        StraddelStop=0
        pnnl = []
        callpos=0
        putpos=0

        for timeData in df.index:
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)

            self.timeData = timeData
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

           #skip times period other than trading hours than ()
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue


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

                # Calculate CE and PE total values
                ce_value = ce_positions["CurrentPrice"].iloc[0]
                pe_value = pe_positions["CurrentPrice"].iloc[0]
                # self.strategyLogger.info(f"ce_value:{ce_value}\tpe_value:{pe_value}")

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

            # ENTRY ORDER FOR INITIAL CONDOR
            if (lastIndexTimeData[1] in df.index) and (self.humanTime.time() < time(14, 15)): 
                
                if (self.humanTime.time() >= time(9,30)) and (flag1==1):
                            
                    prmtb,stike = self.OptChain(lastIndexTimeData[1],"CE",df.at[lastIndexTimeData[1], "c"])
                    otmfactor= self.Otmfactor(prmtb,otmtrade)
                    self.strategyLogger.info(f"otmfactor:{otmfactor}")
                    self.strategyLogger.info(f"prmtb:{prmtb}")

                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=otmfactor)
                    callstrike= callSym[len(callSym) - 7:len(callSym) - 2]
                    HedgeC =float(callstrike)+hedgetrade
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)
                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch,})

                    # entry order for hedge  
                    Hotmfactor=self.Otmfactor(stike,HedgeC)
                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=Hotmfactor)
                    self.strategyLogger.info(f"otmforhedge:{Hotmfactor}")

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch,})
                
                # Put side trades of condor
                if (self.humanTime.time() >= time(9, 30)) and (flag1==1):

                    prmtb,stike = self.OptChain(lastIndexTimeData[1],"PE",df.at[lastIndexTimeData[1], "c"])
                    otmfactor=self.Otmfactor(prmtb,otmtrade)

                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=otmfactor)
                    putstrike= putSym[len(putSym) - 7:len(putSym) - 2]
                    HedgeP =float(putstrike) - hedgetrade
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch,})

                    # entry order for hedge
                    Hotmfactor=self.Otmfactor(stike,HedgeP)
                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=Hotmfactor)
                    self.strategyLogger.info(f"otmforhedge:{Hotmfactor}")

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch,})
                    flag1=0

            #ENTRY ORDER FOR FURTHUR ADJUSTMENTS
            if StraddelStop== 0:
                if callpos==1:
                    prmtb,stike = self.OptChain(lastIndexTimeData[1],"CE",df.at[lastIndexTimeData[1], "c"])
                    otmfactorH= self.Otmfactor(prmtb,data6)  
                    callsymC= int(stike[otmfactorH])

                    if callsymC < putsym1:
                        OTM = (putsym1-callsymC)
                        OTM1 = int(OTM/100)
                        otmfactorH = otmfactorH+OTM1 
                        self.strategyLogger.info(f"OTM1: {OTM1}\totmfactorH: {otmfactorH}")

                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=otmfactorH)
                    callstrike= callSym[len(callSym) - 7:len(callSym) - 2]
                    HedgeC =float(callstrike)+hedgetrade
                    self.strategyLogger.info(f"callSym:{callSym}")

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)   

                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch,})
                    
                    # hedge for the trade  
                    HotmfactorC= self.Otmfactor(stike,HedgeC)
                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=HotmfactorC)
                    self.strategyLogger.info(f"otmforhedge:{HotmfactorC}")

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)


                    self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch,})
                    callpos=0


                if putpos==1:
                    prmtb,stike = self.OptChain(lastIndexTimeData[1],"PE",df.at[lastIndexTimeData[1], "c"])
                    otmfactorH2= self.Otmfactor(prmtb,data5) 
                    putsymp= int(stike[otmfactorH2])

                    if putsymp > callsym1:
                        OTM = (putsymp-callsym1)
                        OTM1= int(OTM/100)
                        otmfactorH2 = otmfactorH2+OTM1 
                        self.strategyLogger.info(f"OTM1: {OTM1}\totmfactorH: {otmfactorH2}")

                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=otmfactorH2)
                    putstrike= putSym[len(putSym) - 7:len(putSym) - 2]
                    HedgeP =float(putstrike) - hedgetrade
                    self.strategyLogger.info(f"putSym:{putSym}")

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)   

                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch,})
                    
                    # hedge for the trade 
                    HotmfactorP= self.Otmfactor(stike,HedgeP) 
                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor=HotmfactorP)
                    self.strategyLogger.info(f"otmforhedge:{HotmfactorP}")

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch,})
                    putpos=0
                    


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
    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2024, 12, 1, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "SENSEX"
    indexName = "SENSEX"

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