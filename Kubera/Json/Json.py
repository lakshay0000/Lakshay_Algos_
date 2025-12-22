import json
import logging
import numpy as np
import pandas as pd
from datetime import time, timedelta
from pandas.api.types import is_datetime64_any_dtype
from backtestTools.histData import getEquityBacktestData, getFnoBacktestData, connectToMongo



def calculate_mtm(closedPnl, saveFileDir, timeFrame="15T", mtm=False, equityMarket=True, conn=None):
    if conn is None:
        conn = connectToMongo()

    if not is_datetime64_any_dtype(closedPnl["Key"]):
        closedPnl["Key"] = pd.to_datetime(closedPnl["Key"])
    if not is_datetime64_any_dtype(closedPnl["ExitTime"]):
        closedPnl["ExitTime"] = pd.to_datetime(closedPnl["ExitTime"])

    startDatetime = closedPnl['Key'].min().replace(hour=9, minute=15)
    endDatetime = (closedPnl['ExitTime'].max()).replace(hour=15, minute=29)

    mtm_df = pd.DataFrame()

    mtm_df["Date"] = pd.date_range(
        start=startDatetime, end=endDatetime, freq="1T")
    mtm_df['Index'] = mtm_df['Date']
    mtm_df.set_index("Index", inplace=True)

    mtm_df = mtm_df.between_time("09:15:00", "15:29:00")
    mtm_df = mtm_df[mtm_df.index.dayofweek < 5]

    mtm_df['ti'] = (mtm_df.index.values.astype(np.int64) //
                    10**9) - 19800
    mtm_df.set_index("ti", inplace=True)

    mtm_df['OpenTrades'] = 0
    mtm_df['CapitalInvested'] = 0
    mtm_df['CumulativePnl'] = 0
    mtm_df['mtmPnl'] = 0
    mtm_df['BuyPosition'] = 0
    mtm_df['SellPosition'] = 0
    mtm_df['BuyMargin'] = 0

    i = 0
    total_rows = len(closedPnl)
    for index, row in closedPnl.iterrows():
        tradeStart = closedPnl.at[index, 'Key'] - \
            timedelta(hours=5, minutes=30)
        tradeEnd = closedPnl.at[index, 'ExitTime'] - \
            timedelta(hours=5, minutes=30)+timedelta(days=1)

        if equityMarket:
            ohlc_df = getEquityBacktestData(
                row["Symbol"], tradeStart, tradeEnd, "T", conn=conn)
        else:
            ohlc_df = getFnoBacktestData(
                row["Symbol"], tradeStart, tradeEnd, "T", conn=conn)

        if ohlc_df is None:
            print(f"No historical data for {row['Symbol']}. Skipping trade.")
            continue

        try:
            # if ohlc_df.at[ohlc_df.index[-1], 'datetime'].date() == row['ExitTime'].date():
            last_index = ohlc_df.index[-1]
            next_index = mtm_df[mtm_df.index > last_index].index[0]
            ohlc_df.loc[next_index] = 0
            ohlc_df.loc[next_index, 'ti'] = next_index
            ohlc_df.loc[next_index, 'datetime'] = mtm_df.at[next_index, "Date"]
        except Exception as e:
            next_index = last_index + 60
            ohlc_df.loc[next_index] = 0
            ohlc_df.loc[next_index, 'ti'] = next_index
            ohlc_df.loc[next_index, 'datetime'] = pd.to_datetime(
                next_index, unit='s')

        ohlc_df['openTrade'] = 1
        ohlc_df['pnl'] = ((ohlc_df['o'] - row["EntryPrice"])
                          * row["Quantity"] * row["PositionStatus"])

        if row["PositionStatus"] == 1:
            ohlc_df['buyPosition'] = 1
            ohlc_df['sellPosition'] = 0
            ohlc_df['buyMargin'] = row["EntryPrice"] * row["Quantity"]
        else:
            ohlc_df['buyPosition'] = 0
            ohlc_df['sellPosition'] = 1
            ohlc_df['buyMargin'] = 0

        ohlc_df.loc[ohlc_df['datetime'] >=
                    row['ExitTime'], 'openTrade'] = 0
        ohlc_df.loc[ohlc_df['datetime'] >=
                    row['ExitTime'], 'pnl'] = closedPnl.at[index, 'Pnl']
        ohlc_df.loc[ohlc_df['datetime'] >=
                    row['ExitTime'], 'buyPosition'] = 0
        ohlc_df.loc[ohlc_df['datetime'] >=
                    row['ExitTime'], 'sellPosition'] = 0
        ohlc_df.loc[ohlc_df['datetime'] >=
                    row['ExitTime'], 'buyMargin'] = 0

        merged_df = pd.merge(
            mtm_df, ohlc_df[['openTrade', 'pnl', 'buyPosition', 'sellPosition', 'buyMargin']],  how="outer", left_index=True, right_index=True)
        merged_df.fillna(method='ffill', inplace=True)
        merged_df.fillna(0, inplace=True)

        mtm_df['OpenTrades'] += merged_df['openTrade']
        mtm_df['CumulativePnl'] += merged_df['pnl']
        mtm_df['BuyPosition'] += merged_df['buyPosition']
        mtm_df['SellPosition'] += merged_df['sellPosition']
        mtm_df['BuyMargin'] += merged_df['buyMargin']

        progress = (i + 1) / total_rows * 100
        print(f"Progress: {progress:.2f}%", end="\r")
        i += 1

    mtm_df['Spread'] = np.minimum(
        mtm_df['BuyPosition'], mtm_df['SellPosition'])
    mtm_df['CapitalInvested'] = (mtm_df['Spread'] * 30000) + (
        (mtm_df['BuyPosition'] - mtm_df['Spread']) * mtm_df['BuyMargin']) + ((mtm_df['SellPosition'] - mtm_df['Spread']) * 100000)

    mtm_df['Index'] = mtm_df['Date']
    mtm_df.set_index("Index", inplace=True)
    mtm_df = mtm_df.resample(timeFrame, origin="9:15").agg(
        {
            "Date": "first",
            "OpenTrades": "max",
            "CapitalInvested": "max",
            "CumulativePnl": "last",
            "mtmPnl": "last",
        }
    )
    mtm_df.dropna(inplace=True)

    mtm_df["Peak"] = mtm_df["CumulativePnl"].cummax()
    mtm_df["Drawdown"] = mtm_df["CumulativePnl"] - mtm_df["Peak"]

    prevDayEndSeries = mtm_df.groupby(mtm_df['Date'].dt.date)[
        'CumulativePnl'].last().shift(1)
    mtm_df['prevDayEndPnl'] = mtm_df['Date'].dt.date.map(prevDayEndSeries)
    mtm_df['mtmPnl'] = mtm_df.loc[mtm_df['Date'].dt.date ==
                                  mtm_df['Date'].dt.date.min(), 'CumulativePnl']
    mask = mtm_df['Date'].dt.date != mtm_df['Date'].dt.date.min()
    mtm_df.loc[mask, 'mtmPnl'] = mtm_df.loc[mask,
                                            'CumulativePnl'] - mtm_df.loc[mask, 'prevDayEndPnl']

    del mtm_df['prevDayEndPnl']

    mtm_df.fillna(0, inplace=True)
    mtm_df.reset_index(drop=True, inplace=True)

    saveFileSplit = saveFileDir.split('/')[::-1]
    saveFileSplitLen = len(saveFileSplit)
    saveFile = None
    for i in range(saveFileSplitLen):
        if '_' in saveFileSplit[i]:
            saveFile = '_'.join(saveFileSplit[:i+1][::-1])
            break

    if saveFile is None:
        saveFile = saveFileDir.split('/')[-1]

    mtm_df.to_csv(f"{saveFileDir}/mtm_{saveFile}.csv")
    print("dailyReport.csv saved")

    closedPnlCopy = closedPnl.copy(deep=True)

    for col in closedPnlCopy.select_dtypes(include=['datetime64[ns]']):
        closedPnlCopy[col] = closedPnlCopy[col].dt.strftime(
            '%Y-%m-%d %H:%M:%S')

    for col in mtm_df.select_dtypes(include=['datetime64[ns]']):
        mtm_df[col] = mtm_df[col].dt.strftime(
            '%Y-%m-%d %H:%M:%S')

    merged_data = {}

    merged_data["closedPnl"] = closedPnlCopy.to_dict(orient='records')
    merged_data["mtm"] = mtm_df.to_dict(orient='records')

    json_data = json.dumps(merged_data)

    with open(f"{saveFileDir}/{saveFile}.json", "w") as outfile:
        outfile.write(json_data)

    return mtm_df



df = pd.read_csv(r'/root/Lakshay_Algos/Kubera/G_L_Indexes/BacktestResults/NA_rdx_v1/9/closePnl_NA_rdx_v1_9.csv')
saveFileDir = r'/root/Lakshay_Algos/Kubera/Json'  

calculate_mtm(df, saveFileDir, timeFrame=timedelta(minutes=1), mtm=True, equityMarket=True)