from backtestTools.util import createPortfolio
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from backtestTools.histData import getEquityBacktestData, getEquityHistData
import talib
import pandas as pd
from termcolor import colored, cprint
from datetime import datetime
from backtestTools.util import setup_logger


class Horizontal50(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "Horizontal50":
            raise Exception("Strategy Name Mismatch")
        cprint(f"Backtesting: {self.strategyName} UID: {self.fileDirUid}", "green")
        first_stock = portfolio[0][0] if portfolio and portfolio[0] else None
        if first_stock:
            self.backtest(first_stock, startDate, endDate)
            print(colored("Backtesting 100% complete.", "light_yellow"))
        else:
            print(colored("No stocks to backtest.", "red"))
        return self.fileDir["backtestResultsStrategyUid"], self.combinePnlCsv()

    def backtest(self, stockName, startDate, endDate):
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()
        stockAlgoLogic = equityOverNightAlgoLogic(stockName, self.fileDir)
        logger = setup_logger(stockName, f"{self.fileDir['backtestResultsStrategyLogs']}/{stockName}.log")
        logger.propagate = False

        stocks = ['OFSS', 'USHAMART', 'ELGIEQUIP', 'CENTURYTEX', 'DCMSHRIRAM', 'TRIVENI', 'PRAJIND', 'JMFINANCIL', 'CHOLAFIN', 'SIEMENS', 'SOBHA', 'DIXON', 'KEC', 'KEI', 'BALRAMCHIN', 'GODREJIND', 'BLUEDART', 'METROPOLIS', 'IEX', 'GODREJPROP', 'TIINDIA', 'HDFCLIFE', 'BOSCHLTD', 'KNRCON', 'DIVISLAB', 'SHRIRAMFIN', 'GRSE', 'APARINDS', 'JKPAPER', 'ESCORTS', 'GUJGASLTD', 'KPRMILL', 'JUSTDIAL', 'BSOFT', 'TIMKEN', 'EIDPARRY', 'MFSL', 'VOLTAS', 'FSL', 'BALAMINES', 'TITAN', 'MAXHEALTH', 'PFC', 'PRESTIGE', 'COFORGE', 'BHEL', 'DEEPAKNTR', 'COCHINSHIP', 'ICICIPRULI', 'LINDEINDIA', 'RCF', 'MINDACORP', 'ABCAPITAL', 'MAZDOCK', 'SCHAEFFLER', 'SOLARINDS', 'M&M', 'SUNDRMFAST', 'OIL', 'ZENSARTECH', 'ASTRAZEN', 'LT', 'ABB', 'IPCALAB', 'HAL', 'MCX', 'ENDURANCE', 'MPHASIS', 'PEL', 'GMRINFRA', 'CARBORUNIV', 'NIACL', 'CHOLAHLDNG', 'AARTIIND', 'GPIL', 'GLENMARK', 'CHAMBLFERT', 'QUESS', 'INTELLECT', 'NAVINFLUOR', 'BLS', 'UPL', '3MINDIA', 'DLF', 'MOTHERSON', 'LALPATHLAB', 'TATAELXSI', 'JINDALSTEL', 'VGUARD', 'CERA', 'MRPL', 'NETWORK18', 'KSB', 'LTIM', 'JSWENERGY', 'BSE', 'BRIGADE', 'KPITTECH', 'BIRLACORPN', 'GLAXO', 'SRF', 'IGL', 'COROMANDEL', 'M&MFIN', 'ACE', 'VBL', 'TECHM', 'CREDITACC', 'CCL', 'BEL', 'WHIRLPOOL', 'NAUKRI', 'BAJFINANCE', 'INDUSINDBK', 'VAIBHAVGBL', 'WIPRO', 'SBICARD', 'FLUOROCHEM', 'ULTRACEMCO', 'GRINDWELL', 'HINDALCO', 'MANAPPURAM', 'MGL', 'CUMMINSIND', 'RECLTD', 'RKFORGE', 'HFCL', 'JINDALSAW', 'BAYERCROP', 'TEJASNET', 'CANFINHOME', 'IBULHSGFIN', 'GPPL', 'LAURUSLABS', 'NMDC', 'CYIENT', 'GSFC', 'WELCORP', 'GILLETTE', 'NESTLEIND', 'TCS', 'RAMCOCEM', 'MAHLIFE', 'TVSMOTOR', 'ALKYLAMINE', 'PERSISTENT', 'PETRONET', 'GRAPHITE', 'MUTHOOTFIN', 'EXIDEIND', 'BPCL', 'HUDCO', 'HDFCBANK', 'CRISIL', 'GICRE', 'TATAINVEST', 'BAJAJFINSV', 'FACT', 'RAJESHEXPO', 'ECLERX', 'AUROPHARMA', 'GESHIP', 'POLYCAB', 'APOLLOHOSP', 'ASTRAL', 'ACC', 'FORTIS', 'KOTAKBANK', 'SPARC', 'INDHOTEL', 'GSPL', 'LICHSGFIN', 'NATIONALUM', 'PHOENIXLTD', 'HAVELLS', 'CHENNPETRO', 'BHARATFORG', 'ADANIPORTS', 'JKLAKSHMI', 'AJANTPHARM', 'CDSL', 'INFY', 'PAGEIND', 'ICICIGI', 'GNFC', 'OBEROIRLTY', 'LTTS', 'SWANENERGY', 'CROMPTON', 'ASTERDM', 'PNBHOUSING', 'ITI', 'DMART', 'MAHSEAMLES', 'INDIANB', 'SYNGENE', 'RELIANCE', 'TATAPOWER', 'HEROMOTOCO', 'AUBANK', 'BEML', 'NATCOPHARM', 'JKCEMENT', 'GAEL', 'ATUL', 'ABBOTINDIA', 'REDINGTON', 'BERGEPAINT', 'RADICO', 'SBIN', 'IOC', 'BANKINDIA', 'BDL', 'HINDCOPPER', 'KRBL', 'MASTEK', 'HBLPOWER', 'HONAUT', 'CGPOWER', 'DRREDDY', 'PIIND', 'HCLTECH', 'JBCHEPHARM', 'ASIANPAINT', 'SUNPHARMA', 'GODFRYPHLP', 'CESC', 'LUPIN', 'APOLLOTYRE', 'INDIACEM', 'BAJAJ-AUTO', 'HINDPETRO', 'ITC', 'ERIS', 'CHALET', 'PGHH', 'PNCINFRA', 'AMBER', 'ASAHIINDIA', 'ADANIENT', 'JSWSTEEL', 'VEDL', 'KANSAINER', 'GAIL', 'CGCL', 'ADANIENSOL', 'BLUESTARCO', 'JSL', 'MRF', 'RATNAMANI', 'KARURVYSYA', 'BATAINDIA', 'MHRIL', 'GMMPFAUDLR', 'GRASIM', 'AMBUJACEM', 'POLYMED', 'BALKRISIND', 'TRITURBINE', 'CEATLTD', 'SUZLON', 'DEEPAKFERT', 'ISEC', 'CIPLA', 'TRENT', 'PIDILITIND', 'AIAENG', 'FDC', 'TATASTEEL', 'CUB', 'HSCL', 'CONCOR', 'ALLCARGO', 'BIOCON', 'ALKEM', 'TATAMOTORS', 'COLPAL', 'IDFCFIRSTB', 'MARUTI', 'VTL', 'BAJAJHLDNG', 'ONGC', 'FINCABLES', 'BHARTIARTL', 'AXISBANK', 'PRSMJOHNSN', 'RBLBANK', 'IDEA', 'J&KBANK', 'CENTURYPLY', 'ASHOKLEY', 'SHREECEM', 'JUBLFOOD', 'ATGL', 'DALBHARAT', 'HDFCAMC', 'INDIGO', 'BANKBARODA', 'UBL', 'NTPC', 'VIPIND', 'GODREJCP', 'SAIL', 'POWERGRID', 'SJVN', 'IDFC', 'APLLTD', 'MMTC', 'HEG', 'TATACHEM', 'TV18BRDCST', 'TATAMTRDVR', 'SAREGAMA', 'YESBANK', 'CASTROLIND', 'ZYDUSLIFE', 'RVNL', 'GMDCLTD', 'SKFINDIA', 'UCOBANK', 'EIHOTEL', 'COALINDIA', 'ABFRL', 'MOTILALOFS', 'SONATSOFTW', 'PATANJALI', 'ICICIBANK', 'IOB', 'IRCTC', 'LEMONTREE', 'CANBK', 'TATACONSUM', 'AVANTIFEED', 'SUNDARMFIN', 'FINPIPE', 'BANDHANBNK','BRITANNIA', 'MAHABANK', 'TORNTPHARM', 'IRCON', 'PNB', 'JBMA', 'UNIONBANK', 'THERMAX', 'GRANULES', 'NHPC', 'RAYMOND', 'POONAWALLA', 'FINEORG', 'JYOTHYLAB', 'SUNTECK', 'ENGINERSIN', 'FEDERALBNK', 'NCC', 'SUPREMEIND', 'IDBI', 'ADANIGREEN', 'IRB', 'EICHERMOT', 'ADANIPOWER', 'SBILIFE', 'INOXWIND', 'TTML', 'TATACOMM', 'ZEEL', 'NBCC', 'TRIDENT', 'VARROC', 'TORNTPOWER', 'KAJARIACER', 'HINDUNILVR', 'NLCINDIA', 'SCHNEIDER', 'DABUR', 'BBTC', 'ELECON', 'OLECTRA', 'NH', 'HINDZINC', 'EMAMILTD', 'TANLA', 'APLAPOLLO', 'SUNTV', 'AAVAS', 'MARICO', 'CAPLIPOINT']

        df = {}
        for stock in stocks:
            df[stock] = getEquityBacktestData(stock, startTimeEpoch - (86400*5), endTimeEpoch, "D")

            if df[stock] is not None:
                df[stock]['datetime'] = pd.to_datetime(df[stock]['datetime'])
                df[stock]['date'] = df[stock]['datetime'].dt.date
                df[stock]['month'] = df[stock]['datetime'].dt.month
                df[stock]['year'] = df[stock]['datetime'].dt.year
                df[stock]['day'] = df[stock]['datetime'].dt.day
                df[stock]['date'] = pd.to_datetime(df[stock]['date'])
                df[stock]['yes'] = ''
                df[stock].index = df[stock].index + 33300

                second_last_indices = df[stock].groupby(df[stock]['date'].dt.year).nth(-2).index
                df[stock].loc[second_last_indices, 'yes'] = 'yes'
                df[stock].dropna(inplace=True)

                df[stock]["rsi"] = talib.RSI(df[stock]["c"], timeperiod=14)
                df[stock]['prev_rsi'] = df[stock]['rsi'].shift(1)
                df[stock]['prev_c'] = df[stock]['c'].shift(1)

                df[stock].dropna(inplace=True)
                df[stock]['timeUp'] = ""
                df[stock].loc[df[stock].index[-1], 'timeUp'] = 'timeUp'
                df[stock].to_csv(f"{self.fileDir['backtestResultsCandleData']}{stock}_df.csv")

        amountPerTrade = 30000
        lastIndexTimeData = None
        breakeven = {}
        TotalTradeCanCome = 50

        for timeData in df['AARTIIND'].index:
            for stock in stocks:
                print(stock)

                stockAlgoLogic.timeData = timeData
                stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)

                stock_openPnl = stockAlgoLogic.openPnl[stockAlgoLogic.openPnl['Symbol'] == stock]

                if not stock_openPnl.empty:
                    for index, row in stock_openPnl.iterrows():
                        try:
                            stockAlgoLogic.openPnl.at[index, 'CurrentPrice'] = df[stock].at[lastIndexTimeData, "c"]
                        except Exception as e:
                            print(f"Error fetching historical data for {row['Symbol']}")
                stockAlgoLogic.pnlCalculator()

                for index, row in stock_openPnl.iterrows():
                    if lastIndexTimeData in df[stock].index:
                        if index in stock_openPnl.index:

                            if df[stock].at[lastIndexTimeData, "timeUp"] == "timeUp":
                                exitType = "TimeUpExit"
                                stockAlgoLogic.exitOrder(index, exitType, df[stock].at[lastIndexTimeData, "c"])

                            elif df[stock].at[lastIndexTimeData, "rsi"] < 30 and df[stock].at[lastIndexTimeData, "c"] > row['EntryPrice']:
                                exitType = "TargetUsingRsi"
                                stockAlgoLogic.exitOrder(index, exitType, df[stock].at[lastIndexTimeData, "c"])

                if lastIndexTimeData in df[stock].index:

                    nowTotalTrades = len(stockAlgoLogic.openPnl)
                    if df[stock].at[lastIndexTimeData, "rsi"] > 60 and stock_openPnl.empty and nowTotalTrades < TotalTradeCanCome:

                        entry_price = df[stock].at[lastIndexTimeData, "c"]
                        breakeven[stock] = False
                        quantity = (amountPerTrade // entry_price)
                        stockAlgoLogic.entryOrder(entry_price, stock, quantity, "BUY", {"quantity": (amountPerTrade // entry_price)})

                lastIndexTimeData = timeData
                stockAlgoLogic.pnlCalculator()

        for index, row in stockAlgoLogic.openPnl.iterrows():
            if lastIndexTimeData in df[stock].index:
                if index in stockAlgoLogic.openPnl.index:
                        exitType = "TimeUpExit" 
                        stockAlgoLogic.exitOrder(index, exitType, df[stock].at[lastIndexTimeData, "c"])

if __name__ == "__main__":
    startNow = datetime.now()

    devName = "NA"
    strategyName = "Horizontal50"
    version = "v1"

    startDate = datetime(2019, 1, 1, 9, 15)
    endDate = datetime(2024, 12, 31, 15, 30)

    portfolio = createPortfolio("/root/BacktestHunain/stocksList/tes1.md",1)

    algoLogicObj = Horizontal50(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")