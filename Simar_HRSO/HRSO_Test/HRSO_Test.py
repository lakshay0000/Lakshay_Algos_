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
            df_1d = getFnoBacktestData(indexSym, startEpoch-(86400*50), endEpoch, "1D")
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_1d.dropna(inplace=True)

        # # Add 33360 to the index to match the timestamp
        df_1d.index = df_1d.index + 33300
        df_1d.ti = df_1d.ti + 33300

        df_1d = df_1d[df_1d.index >= (startEpoch-(86400*5))]


        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_1d.to_csv(
                f"{self.fileDir['backtestResultsCandleData']}{indexName}_1d.csv"
            )
        
        


        lastIndexTimeData = [0, 0]

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        df_CE = None
        df_1d_CE = None
        df_PE = None
        df_1d_PE = None

        CE_Target = False
        PE_Target = False

        PE_High = 70
        PE_Low = 30

        CE_High = 70
        CE_Low = 30


        

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # # Skip the dates 2nd March 2024 and 18th May 2024
            # if self.humanTime.date() == datetime(2025, 4, 7).date() or self.humanTime.date() == datetime(2025, 6, 16).date():
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
            

            if self.humanTime.date() == expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData, baseSym)['NextExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
                CE_Target = False
                PE_Target = False
                

            # if self.humanTime.date() != (expiryDatetime).date():
            #     continue


            if self.humanTime.time() == time(9, 17):
                #Updating daily index
                open_epoch = lastIndexTimeData[1]
                # prev_day = lastIndexTimeData[1] - 86400
                # #check if previoud day exists in 1d data
                # while prev_day not in df_1d.index:
                #     prev_day = prev_day - 86400

                callSym = self.getCallSym(
                        self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=0)
                    
                putSym = self.getPutSym(
                    self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=0)


                df_CE = getFnoBacktestData(callSym, open_epoch, open_epoch + 86400, "1Min")
                df_CE['High'] = df_CE['h'].cummax()
                df_CE['Low'] = df_CE['l'].cummin()
                df_CE['range'] = df_CE['High'] - df_CE['Low']
                df_CE['HRSO'] = ((df_CE['c'] - df_CE['Low']) / df_CE['range'])*100
                self.strategyLogger.info(f"{self.humanTime} {callSym} df_CE:\n{df_CE.head(350).to_string()}")
                # self.strategyLogger.info(f"{self.humanTime} df_CE (first 20 rows):\n{df_CE.head(350).to_string()} {callSym}")

                df_PE = getFnoBacktestData(putSym, open_epoch, open_epoch + 86400, "1Min")
                df_PE['High'] = df_PE['h'].cummax()
                df_PE['Low'] = df_PE['l'].cummin()
                df_PE['range'] = df_PE['High'] - df_PE['Low']
                df_PE['HRSO'] = ((df_PE['c'] - df_PE['Low']) / df_PE['range'])*100
                self.strategyLogger.info(f"{self.humanTime} {putSym} df_PE:\n{df_PE.head(350).to_string()}")

                
                # pe_prev_day_high = df_1d_PE['h'].max()
                # pe_prev_day_low = df_1d_PE['l'].min()
                # ce_prev_day_high = df_1d_CE['h'].max()
                # ce_prev_day_low = df_1d_CE['l'].min()
                # self.strategyLogger.info(f"pe_prev_day_high:{pe_prev_day_high}")
                # self.strategyLogger.info(f"pe_prev_day_low:{pe_prev_day_low}")
                # self.strategyLogger.info(f"ce_prev_day_high:{ce_prev_day_high}")
                # self.strategyLogger.info(f"ce_prev_day_low:{ce_prev_day_low}")


            # Check if the current time is past the expiry time
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
            
            # if not self.openPnl.empty:
            #     Current_strangle_value = self.openPnl['CurrentPrice'].sum()
            #     open_sum = self.openPnl['Pnl'].sum()
            #     pnnl_sum = sum(pnnl) 
            #     self.strategyLogger.info(f"pnl_sum:{open_sum + pnnl_sum}")

            #     if (open_sum + pnnl_sum) <= -6000:
            #         for index, row in self.openPnl.iterrows():
            #             self.exitOrder(index, "MaxLoss")
            #             EntryAllowed = False
            #             pnnl = []
            #             i = 3
            #             i_CanChange = False


            # # First, check all positions for stoploss
            # if not self.openPnl.empty and (Stranggle_Exit == False):
            #     for index, row in self.openPnl.iterrows():
            #         if row["CurrentPrice"] >= row["Stoploss"]:
            #             Stranggle_Exit = True
            #             if i_CanChange:
            #                 if i < 5:
            #                     i += 1
            #                     self.strategyLogger.info(f"i value increased to {i}")
            #                 else:
            #                     i = 5
            #                     self.strategyLogger.info(f"i value remains {i}")
            #                 i_CanChange = False
            
            # if self.humanTime.time() > time(9, 17):
            #     if (lastIndexTimeData[1] in df_CE.index):



            # Check for exit conditions and execute exit orders
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]      

                    if self.humanTime.time() >= time(15, 20):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)

                    elif symSide == 'CE':
                        if row["CurrentPrice"] <= row["Target"]:
                            self.openPnl.at[index, "stoploss"] = (df_CE.loc[:lastIndexTimeData[1]-60, 'c'].min())*2
                            self.openPnl.at[index, "Target"] = row["CurrentPrice"]
                            self.strategyLogger.info(f"{self.humanTime} TARGET HIT CE and stoploss shifted to: {self.openPnl.at[index, 'stoploss']}")
                            self.strategyLogger.info(f"Target: {self.openPnl.at[index, 'Target']}")
                            self.strategyLogger.info(f"{self.openPnl[['Symbol', 'Target', 'stoploss']].to_string()}")
                            CE_Target = True

                        elif row["CurrentPrice"] >= row["stoploss"]:
                            exitType = "CE_Stoploss Hit"
                            self.exitOrder(index, exitType)

                        elif (lastIndexTimeData[1] in df_CE.index):
                            if df_CE.at[lastIndexTimeData[1], "HRSO"] > CE_High:
                                exitType = "CE_high_Break"
                                self.exitOrder(index, exitType)

                        
                    elif symSide == 'PE':
                        if row["CurrentPrice"] <= row["Target"]:
                           self.openPnl.at[index, "stoploss"] = (df_PE.loc[:lastIndexTimeData[1]-60, 'c'].min())*2
                           self.openPnl.at[index, "Target"] = row["CurrentPrice"]
                           self.strategyLogger.info(f"{self.humanTime} TARGET HIT PE and stoploss shifted to: {self.openPnl.at[index, 'stoploss']}")
                           self.strategyLogger.info(f"Target: {self.openPnl.at[index, 'Target']}")
                           self.strategyLogger.info(f"{self.openPnl[['Symbol', 'Target', 'stoploss']].to_string()}")
                           PE_Target = True


                        elif row["CurrentPrice"] >= row["stoploss"]:
                            exitType = "PE_Stoploss Hit"
                            self.exitOrder(index, exitType)


                        elif (lastIndexTimeData[1] in df_PE.index):
                            if df_PE.at[lastIndexTimeData[1], "HRSO"] > PE_High:
                                exitType = "PE_high_Break"
                                self.exitOrder(index, exitType)



            tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            callCounter= tradecount.get('CE',0)
            putCounter= tradecount.get('PE',0)




            # Check for entry signals and execute orders
            if ((timeData-60) in df.index) and self.humanTime.time() < time(15, 20) and self.humanTime.time() > time(9, 16):

                if (lastIndexTimeData[1] in df_CE.index) and callCounter < 1:
                    if df_CE.at[lastIndexTimeData[1], "HRSO"] < CE_Low and CE_Target == False:

                        entry_price = df_CE.at[lastIndexTimeData[1], "c"]
                        target = 0.2 * entry_price
                        stoploss = 1.5 * entry_price

                        self.entryOrder(entry_price, callSym, lotSize, "SELL", {"Expiry": expiryEpoch,"Target": target,"stoploss":stoploss},)


                if (lastIndexTimeData[1] in df_PE.index) and putCounter < 1:
                    if df_PE.at[lastIndexTimeData[1], "HRSO"] < PE_Low and PE_Target == False:
                    
                        entry_price = df_PE.at[lastIndexTimeData[1], "c"]
                        target = 0.2 * entry_price
                        stoploss = 1.5 * entry_price

                        self.entryOrder(entry_price, putSym, lotSize, "SELL", {"Expiry": expiryEpoch,"Target": target,"stoploss":stoploss},)




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
    startDate = datetime(2026, 1, 13, 9, 15)
    endDate = datetime(2026, 1, 14, 15, 30)

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