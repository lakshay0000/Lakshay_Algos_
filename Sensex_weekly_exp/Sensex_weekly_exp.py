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

    def straddle(self, date, IndexPrice, baseSym):
        Factor = [0, 1, -1]
        minlist = []
        Strad_list = []

        for i in Factor:
            callSym = self.getCallSym(date, baseSym, IndexPrice, otmFactor=i)
            try:
                dataCE = self.fetchAndCacheFnoHistData(callSym, date)
            except Exception as e:
                self.strategyLogger.info(e)
                return None  # Return None if call data can't be fetched

            if i != 0:
                i_put = -i  # Ensure otmFactor is positive for put options
            else:
                i_put = 0
            putSym = self.getPutSym(date, baseSym, IndexPrice, otmFactor=i_put)
            try:
                dataPE = self.fetchAndCacheFnoHistData(putSym, date)
            except Exception as e:
                self.strategyLogger.info(e)
                return None  # Return None if put data can't be fetched

            Diff = abs(dataCE["c"] - dataPE["c"])
            Sum = dataCE["c"] + dataPE["c"]
            minlist.append(Diff)
            Strad_list.append(Sum)

        # Find the index of the minimum difference
        self.strategyLogger.info(f"Min List: {minlist}")
        self.strategyLogger.info(f"Straddle List: {Strad_list}")
        minIndex = np.argmin(minlist)
        self.strategyLogger.info(f"Minimum Index: {minIndex}")
        prm = Strad_list[minIndex]

        return prm
    
    def OptChain(self, date, symbol, IndexPrice, baseSym):
        prmtb=[]
        if (symbol== "CE"):
            for i in range(0,20):
                callSymotm = self.getCallSym(date, baseSym, IndexPrice, otmFactor=i)         
                try:
                    data = self.fetchAndCacheFnoHistData(
                        callSymotm, date)
                    prmtb.append(data["c"])
                except Exception as e:
                    self.strategyLogger.info(e)
                
                # callstrikeP= callSymotm[len(callSymotm) - 7:len(callSymotm) - 2]
                # callstrikep=float(callstrikeP)

                # prmtb.append(data["c"])   
                # stike.append(callstrikep)    

        if (symbol== "PE"):
            for i in range(0,20):
                putSymotm = self.getPutSym(date, baseSym, IndexPrice, otmFactor=i)
                try:
                    data = self.fetchAndCacheFnoHistData(
                        putSymotm, date)
                    prmtb.append(data["c"])
                except Exception as e:
                    self.strategyLogger.info(e)

                # putstrikeP= putSymotm[len(putSymotm) - 7:len(putSymotm) - 2]
                # putstrikep=float(putstrikeP)

                # prmtb.append(data["c"])
                # stike.append(putstrikep)

        return prmtb
        
    def Otmfactor(self, premiumlst, data):
        nearest_premium = min(premiumlst, key=lambda x: abs(x - data))
        premium_index = premiumlst.index(nearest_premium)
        otmfactor= premium_index

        return otmfactor

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

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        i = 3
        Stranggle_Exit = False
        prev_day = None
        pnnl=[]
        EntryAllowed = True


        

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # Skip the dates 2nd March 2024 and 18th May 2024
            # if self.humanTime.date() == datetime(2024, 4, 26).date() or self.humanTime.date() == datetime(2025, 6, 17).date():
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

            #  # Log relevant information
            # if lastIndexTimeData[1] in df.index:
            #     self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")


            # Update current price for open positions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)

            # Calculate and update PnL
            self.pnlCalculator()
            

            if self.humanTime.date() > expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
                prev_day = None
                i = 3
                # EntryAllowed = True


            # # Check if the current time is past the expiry time
            # if prev_day is None:
            #     prev_day = expiryEpoch - 86400
            #     if timeData in df.index:
            #         #check if previoud day exists in 1d data
            #         while prev_day not in df.index:
            #             prev_day = prev_day - 86400                
            

            # if lastIndexTimeData[1] in df.index:
            #     try:
            #         # for call
            #         data = self.fetchAndCacheFnoHistData(St_CallSym, lastIndexTimeData[1])
            #         a= data["c"]
            #         # for put 
            #         data = self.fetchAndCacheFnoHistData(St_PutSym, lastIndexTimeData[1])
            #         b= data["c"]
            #         Current_strangle_value = a + b
            #     except Exception as e:
            #         self.strategyLogger.info(e)
            
            if not self.openPnl.empty:
                Current_strangle_value = self.openPnl['CurrentPrice'].sum()
                # open_sum = self.openPnl['Pnl'].sum()
                # pnnl_sum = sum(pnnl) 
                # self.strategyLogger.info(f"pnl_sum:{open_sum + pnnl_sum}")

                # if (open_sum + pnnl_sum) <= -10000:
                #     for index, row in self.openPnl.iterrows():
                #         self.exitOrder(index, "MaxLoss")
                #         EntryAllowed = False
                #         pnnl = []
                #         i = 3
                #         i_CanChange = False

            # First, check all positions for stoploss
            if not self.openPnl.empty and (Stranggle_Exit == False):
                for index, row in self.openPnl.iterrows():
                    if row["CurrentPrice"] >= row["Stoploss"]:
                        Stranggle_Exit = True
                        if i_CanChange:
                            if i < 5:
                                i += 1
                                self.strategyLogger.info(f"i value increased to {i}")
                            else:
                                i = 5
                                self.strategyLogger.info(f"i value remains {i}")
                            i_CanChange = False


            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty and (Stranggle_Exit == False):
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]      


                    if Current_strangle_value >= 1.4 * strangle:
                        exitType = "Combined Loss Exit"
                        # pnl = row["Pnl"] 
                        # pnnl.append(pnl)
                        self.exitOrder(index, exitType)
                        self.strategyLogger.info(f"Current_strangle_value:{Current_strangle_value}")
                        if i_CanChange:
                            if i > 1:
                                i = i - 1
                                self.strategyLogger.info(f"i value decreased to {i}")
                            else:
                                i = 1
                                self.strategyLogger.info(f"i value remanins {i}")

                            i_CanChange = False


                    elif Current_strangle_value <= 0.6 * strangle:
                        exitType = "Combined Profit Exit"
                        # pnl = row["Pnl"] 
                        # pnnl.append(pnl)
                        self.exitOrder(index, exitType)
                        self.strategyLogger.info(f"Current_strangle_value:{Current_strangle_value}")
                        if i_CanChange:
                            if i < 5:
                                i += 1
                                self.strategyLogger.info(f"i value increased to {i}")
                            else:
                                i= 5
                                self.strategyLogger.info(f"i value remanins {i}")

                            i_CanChange = False
                        

                    elif self.humanTime.time() >= time(15, 20):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)
                        i = 3
                        i_CanChange = False
                        # pnnl = []
                        self.strategyLogger.info(f"i value reset to {i}")


            if Stranggle_Exit == True:
                for index, row in self.openPnl.iterrows():
                    # pnl = row["Pnl"] 
                    # pnnl.append(pnl)
                    self.exitOrder(index, "DOUBLE STOPLOSS HIT")
                    Stranggle_Exit= False




            # Check for entry signals and execute orders
            if ((timeData-60) in df.index) and self.openPnl.empty:

                if self.humanTime.date() == expiryDatetime.date() and self.humanTime.time() < time(15, 20):
                    straddle_value = self.straddle(lastIndexTimeData[1], df.at[lastIndexTimeData[1], "c"], baseSym)
                    if straddle_value is None:
                        self.strategyLogger.info("Straddle value is None, skipping entry.")
                        continue
                    self.strategyLogger.info(f"Straddle Value: {straddle_value}")
                    strangle_value = straddle_value/(i*2)
                    self.strategyLogger.info(f"Strangle Value: {strangle_value}")

                    prmtb = self.OptChain(lastIndexTimeData[1], "CE", df.at[lastIndexTimeData[1], "c"], baseSym)
                    self.strategyLogger.info(f"Premium List: {prmtb}")
                    otmfactorCE = self.Otmfactor(prmtb, strangle_value)
                    self.strategyLogger.info(f"OTM Factor for CE: {otmfactorCE}")

                    callSym = self.getCallSym(
                        self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=otmfactorCE)
                    
                    St_CallSym = callSym

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    dataCE = data["c"]
                    stoploss = 2 * data["c"]

                    self.entryOrder(data["c"], callSym, lotSize*i, "SELL", {"Expiry": expiryEpoch, "Stoploss": stoploss},)
                    
                    prmtb = self.OptChain(lastIndexTimeData[1], "PE", df.at[lastIndexTimeData[1], "c"], baseSym)
                    self.strategyLogger.info(f"Premium List: {prmtb}")
                    otmfactorPE = self.Otmfactor(prmtb, strangle_value)
                    self.strategyLogger.info(f"OTM Factor for PE: {otmfactorPE}")
                    putSym = self.getPutSym(
                        self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=otmfactorPE)

                    St_PutSym = putSym

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    dataPE = data["c"]
                    stoploss = 2 * data["c"]
                    
                    strangle = dataCE + dataPE

                    self.entryOrder(data["c"], putSym, lotSize*i, "SELL", {"Expiry": expiryEpoch, "Stoploss": stoploss},)
                    i_CanChange = True



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
    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2025, 7, 31, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "SENSEX"
    indexName = "SENSEX"

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