import numpy as np
import talib as ta
import pandas_ta as taa
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getFnoBacktestData, connectToMongo
import sys
import os

# sys.path.insert(1, '/root/backtestTools')


# Define a class algoLogic that inherits from optOverNightAlgoLogic
class algoLogic(optOverNightAlgoLogic):

    def execute_rolling_entry_with_exit_v2(self, symbol, side, entry_price, target, stoploss, expiryEpoch, lotSize):
        """
        Maintain exactly 4 total positions per side (CE/PE)
        
        Logic:
        - 0 positions: Entry 4 in new symbol
        - 1-3 positions: Exit half from max, Enter half in new symbol
        - 4 positions with 4 unique symbols: BLOCKED
        - 4 positions with <4 unique: Exit half from max, Enter half in new symbol
        """
        
        # Get all open positions for this side
        side_positions = self.openPnl[self.openPnl['Symbol'].str.endswith(side)]
        total_positions = len(side_positions)
        
        if total_positions == 0:
            # FIRST ENTRY: Take 4 positions
            self.strategyLogger.info(
                f"{self.humanTime} {side}: NO POSITIONS - Taking 4 lots in {symbol}"
            )
            
            for i in range(4):
                try:
                    self.entryOrder(entry_price, symbol, lotSize, "SELL",
                                {"Expiry": expiryEpoch, "Target": target, "stoploss": stoploss})
                    self.strategyLogger.info(
                        f"{self.humanTime} {side} ENTRY: Lot {i+1}/4 in {symbol} @ {entry_price}"
                    )
                except Exception as e:
                    self.strategyLogger.info(f"Error on entry {i+1}: {str(e)}")
        
        else:
            # Already have positions - check unique symbol count
            unique_symbols = side_positions['Symbol'].nunique()
            
            # Check if we're at 4 positions with 4 unique symbols (1 each)
            if total_positions == 4 and unique_symbols == 4:
                self.strategyLogger.info(
                    f"{self.humanTime} {side}: BLOCKED - Have 4 positions across 4 unique symbols. "
                    f"Wait for one to close completely before taking new entry."
                )
                return
            
            # Find symbol with MOST positions
            symbol_counts = side_positions['Symbol'].value_counts()
            max_symbol = symbol_counts.index[0]
            max_count = symbol_counts.values[0]
            
            # Exit HALF of the max symbol
            exit_qty = max_count // 2
            
            # Entry qty = exit qty (to maintain total of 4)
            entry_qty = exit_qty
            
            self.strategyLogger.info(
                f"{self.humanTime} {side}: ENTRY SIGNAL - "
                f"Total positions: {total_positions}, Unique symbols: {unique_symbols} - "
                f"Exit {exit_qty} from {max_symbol} (has {max_count}), Entry {entry_qty} in {symbol}"
            )
            
            # EXECUTE EXITS FIRST
            if exit_qty > 0:
                matching_rows = self.openPnl[self.openPnl['Symbol'] == max_symbol]
                exit_count = 0
                
                for index, row in matching_rows.iterrows():
                    if exit_count < exit_qty:
                        try:
                            self.exitOrder(index, f"Rolling_Exit_For_{symbol}")
                            self.strategyLogger.info(
                                f"{self.humanTime} {side} EXIT: Lot {exit_count+1}/{exit_qty} "
                                f"from {max_symbol} @ {row['CurrentPrice']}"
                            )
                            exit_count += 1
                        except Exception as e:
                            self.strategyLogger.info(f"Error on exit: {str(e)}")
            
            # EXECUTE ENTRIES
            if entry_qty > 0:
                for i in range(entry_qty):
                    try:
                        self.entryOrder(entry_price, symbol, lotSize, "SELL",
                                    {"Expiry": expiryEpoch, "Target": target, "stoploss": stoploss})
                        self.strategyLogger.info(
                            f"{self.humanTime} {side} ENTRY: Lot {i+1}/{entry_qty} "
                            f"in {symbol} @ {entry_price}"
                        )
                    except Exception as e:
                        self.strategyLogger.info(f"Error on entry {i+1}: {str(e)}")


    

    def FetchCallPut_df(self, baseSym, df, lastIndexTimeData, Currentexpiry, otmFactor, open_epoch):
        callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=otmFactor)
        
        try:
            df_CE = getFnoBacktestData(callSym, open_epoch, open_epoch + 86400, "1Min")
            self.strategyLogger.info(f"{self.humanTime} {callSym}")
            df_CE['Symbol'] = callSym
            df_CE['High'] = df_CE['c'].cummax()
            df_CE['Low'] = df_CE['c'].cummin()
            df_CE['range'] = df_CE['High'] - df_CE['Low']
            df_CE['HRSO'] = ((df_CE['c'] - df_CE['Low']) / df_CE['range'])*100
            # self.strategyLogger.info(f"{self.humanTime} {callSym} df_CE loaded successfully")
            self.strategyLogger.info(f"{self.humanTime} {callSym} df_CE:\n{df_CE.head(350).to_string()}")
        except Exception as e:
            self.strategyLogger.info(f"{self.humanTime} Error fetching CE data for {callSym} {df_CE}: {str(e)}")
            df_CE = None

                
        # Fetch Put DataFrame separately
        putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=otmFactor)
        
        try:
            df_PE = getFnoBacktestData(putSym, open_epoch, open_epoch + 86400, "1Min")
            self.strategyLogger.info(f"{self.humanTime} {putSym}")
            df_PE['Symbol'] = putSym
            df_PE['High'] = df_PE['c'].cummax()
            df_PE['Low'] = df_PE['c'].cummin()
            df_PE['range'] = df_PE['High'] - df_PE['Low']
            df_PE['HRSO'] = ((df_PE['c'] - df_PE['Low']) / df_PE['range'])*100
            # self.strategyLogger.info(f"{self.humanTime} {putSym} df_PE loaded successfully")
            self.strategyLogger.info(f"{self.humanTime} {putSym} df_PE:\n{df_PE.head(350).to_string()}")
        except Exception as e:
            self.strategyLogger.info(f"{self.humanTime} Error fetching PE data for {putSym} {df_PE}: {str(e)}")
            df_PE = None

        return df_CE,df_PE,callSym,putSym

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

        Symbol_1min_data = {}
        stock_state = {}
        otmFactor = 0

        Stk_limit = 0


        

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # # Skip the dates 2nd March 2024 and 18th May 2024
            if self.humanTime.date() == datetime(2024, 1, 5).date():
                continue

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
                CE_Target = False
                PE_Target = False
                df_CE = None  # Reset for next day
                df_PE = None  # Reset for next day
                ATM_stk = None
                Low_Range = None
                High_Range = None
                Symbol_1min_data = {}
                stock_state = {}
                

            if self.humanTime.date() < (expiryDatetime).date():
                continue

            if self.humanTime.time() == time(9, 16):
                open_epoch = lastIndexTimeData[1]
                self.strategyLogger.info(f"{self.humanTime} otmFactor={otmFactor}")


            if self.humanTime.time() >= time(9, 21) and self.humanTime.time() < time(15, 20):
                # if self.humanTime.time() == time(9, 17):
                #     open_epoch = lastIndexTimeData[1]
                #     self.strategyLogger.info(f"{self.humanTime} otmFactor=0")
                
                if self.humanTime.time() == time(9, 21):
                    TsetSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=0)
                    Low_Range = int(TsetSym[len(TsetSym) - 7:len(TsetSym) - 2]) - 50
                    High_Range = int(TsetSym[len(TsetSym) - 7:len(TsetSym) - 2]) + 50
                    self.strategyLogger.info(f"{self.humanTime} Low_Range:{Low_Range} High_Range:{High_Range}")
                                        
                    # Fetch Call DataFrame separately
                    df_CE, df_PE, callSym, putSym = self.FetchCallPut_df(baseSym, df, lastIndexTimeData, Currentexpiry, otmFactor, open_epoch)

                    if df_CE is not None:
                        Symbol_1min_data[callSym] = df_CE
                        stock_state[callSym] = {"Target": False}
                    
                    if df_PE is not None:
                        Symbol_1min_data[putSym] = df_PE
                        stock_state[putSym] = {"Target": False}


                else:
                    if lastIndexTimeData[1] in df.index:
                        CSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=0) 
                        PSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=0)
                        stk = int(CSym[len(CSym) - 7:len(CSym) - 2])
                        
                        if df.at[lastIndexTimeData[1], "c"] <= Low_Range or df.at[lastIndexTimeData[1], "c"] >= High_Range:
                            Low_Range = stk - 50
                            High_Range = stk + 50
                            self.strategyLogger.info(f"{self.humanTime} Low_Range changed to:{Low_Range} High_Range changed to:{High_Range}")
                            
                            # Fetch Call DataFrame separately
                            if CSym not in Symbol_1min_data and PSym not in Symbol_1min_data:
                                df_CE, df_PE, callSym, putSym = self.FetchCallPut_df(baseSym, df, lastIndexTimeData, Currentexpiry, otmFactor, open_epoch)
                                Stk_limit = Stk_limit + 1

                                if df_CE is not None:
                                    Symbol_1min_data[callSym] = df_CE
                                    stock_state[callSym] = {"Target": False}
                                
                                if df_PE is not None:
                                    Symbol_1min_data[putSym] = df_PE
                                    stock_state[putSym] = {"Target": False}

                    

                
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

                    df_opt = Symbol_1min_data.get(row["Symbol"])
                    state = stock_state[row["Symbol"]]


                    if self.humanTime.time() >= time(15, 20):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)

                    elif symSide == 'CE':
                        if row["CurrentPrice"] <= row["Target"]:
                            exitType = "Target Hit"
                            # self.openPnl.at[index, "stoploss"] = (df_opt.loc[:lastIndexTimeData[1]-60, 'c'].min())*2
                            # self.openPnl.at[index, "Target"] = row["CurrentPrice"]
                            # self.strategyLogger.info(f"{self.humanTime} TARGET HIT CE and stoploss shifted to: {self.openPnl.at[index, 'stoploss']}")
                            # self.strategyLogger.info(f"Target: {self.openPnl.at[index, 'Target']}")
                            # self.strategyLogger.info(f"{self.openPnl[['Symbol', 'Target', 'stoploss']].to_string()}")
                            self.exitOrder(index, exitType)
                            state["Target"] = True

                        elif row["CurrentPrice"] >= row["stoploss"]:
                            exitType = "CE_Stoploss Hit"
                            self.exitOrder(index, exitType)

                        elif (lastIndexTimeData[1] in df_opt.index):
                            if df_opt.at[lastIndexTimeData[1], "HRSO"] > CE_High:
                                exitType = "CE_high_Break"
                                self.exitOrder(index, exitType)

                        
                    elif symSide == 'PE':
                        if row["CurrentPrice"] <= row["Target"]:
                           exitType = "Target Hit"
                           #self.openPnl.at[index, "stoploss"] = (df_opt.loc[:lastIndexTimeData[1]-60, 'c'].min())*2
                           #self.openPnl.at[index, "Target"] = row["CurrentPrice"]
                           #self.strategyLogger.info(f"{self.humanTime} TARGET HIT PE and stoploss shifted to: {self.openPnl.at[index, 'stoploss']}")
                           #self.strategyLogger.info(f"Target: {self.openPnl.at[index, 'Target']}")
                           #self.strategyLogger.info(f"{self.openPnl[['Symbol', 'Target', 'stoploss']].to_string()}")
                           self.exitOrder(index, exitType)
                           state["Target"] = True


                        elif row["CurrentPrice"] >= row["stoploss"]:
                            exitType = "PE_Stoploss Hit"
                            self.exitOrder(index, exitType)


                        elif (lastIndexTimeData[1] in df_opt.index):
                            if df_opt.at[lastIndexTimeData[1], "HRSO"] > PE_High:
                                exitType = "PE_high_Break"
                                self.exitOrder(index, exitType)



            # tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            # callCounter= tradecount.get('CE',0)
            # putCounter= tradecount.get('PE',0)




            # Check for entry signals and execute orders
            if ((timeData-60) in df.index) and self.humanTime.time() < time(15, 20) and self.humanTime.time() > time(9, 20):

                for symbol, df_opt in Symbol_1min_data.items():

                    if lastIndexTimeData[1] not in df_opt.index:
                        continue
                    
                    state = stock_state[symbol]
                    symSide = symbol[len(symbol) - 2:]  # 'CE' or 'PE'

                    tradecount = self.openPnl['Symbol'].value_counts()
                    sym_count = tradecount.get(symbol, 0)
                    
                    if sym_count == 0:
                        if symSide == 'CE':
                            if df_opt.at[lastIndexTimeData[1], "HRSO"] < CE_Low and state["Target"] == False:
                                
                                entry_price = df_opt.at[lastIndexTimeData[1], "c"]
                                target = 0.3 * entry_price
                                stoploss = 1.5 * entry_price

                                # Rolling strategy - maintains exactly 4 positions per side
                                self.execute_rolling_entry_with_exit_v2(
                                    symbol, 'CE', entry_price, target, stoploss, expiryEpoch, lotSize
                                )

                        elif symSide == 'PE':
                            if df_opt.at[lastIndexTimeData[1], "HRSO"] < PE_Low and state["Target"] == False:
                                
                                entry_price = df_opt.at[lastIndexTimeData[1], "c"]
                                target = 0.3 * entry_price
                                stoploss = 1.5 * entry_price

                                # Rolling strategy - maintains exactly 4 positions per side
                                self.execute_rolling_entry_with_exit_v2(
                                    symbol, 'PE', entry_price, target, stoploss, expiryEpoch, lotSize
                                )




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
    endDate = datetime(2025, 12, 31, 15, 30)

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