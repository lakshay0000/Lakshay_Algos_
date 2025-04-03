import pandas as pd
import numpy as np

df = pd.read_csv("closePnl_NA_ManjeetMergedStrategy_10Am_v1_5_2023_newstockList (1).csv")
df["Key"] = pd.to_datetime(df["Key"], errors='coerce')
df["Date"] = df["Key"].dt.date
df = df.sort_values(by=["Date", "Symbol"], ascending=[True, True])
df.to_csv("manjeet1.csv")

df_1 = pd.read_csv("closePnl_NA_EmafiveSell_v1_1 (1).csv")
df_1["Key"] = pd.to_datetime(df_1["Key"], errors='coerce')
df_1["Date"] = df_1["Key"].dt.date
df_1 = df_1.sort_values(by=["Date", "Symbol"], ascending=[True, True])
df_1.to_csv("manjeet2.csv")


