import pandas as pd
import matplotlib.pyplot as plt

# Load your CSV file (update the path as needed)
df = pd.read_csv("/root/Lakshay_Algos/Chart_ATR/BacktestResults/NA_rdx_v1/2/CandleData/ADANIENT_1Min.csv")

# Ensure datetime column is in datetime format
df['datetime'] = pd.to_datetime(df['datetime'])

# Plot ATR vs Date
plt.figure(figsize=(12, 6))
plt.plot(df['datetime'], df['ATR'], marker='o', linestyle='-', color='blue')
plt.title('Daily ATR Line Chart')
plt.xlabel('Date')
plt.ylabel('ATR')
plt.grid(True)
plt.tight_layout()
plt.show()
plt.savefig('daily_atr_chart.png')