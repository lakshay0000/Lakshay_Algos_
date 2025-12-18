import pandas as pd

df = pd.read_csv("/root/Lakshay_Algos/uplod.csv")

df['ExitType'] = "ExitType"

df.to_csv('msid.csv')