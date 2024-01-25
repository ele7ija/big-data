

import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.dates as mdates
import datetime as dt
import math

df = pd.read_csv("query_data/result.csv", header=0)

x = pd.to_datetime(df['Date'])

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=math.ceil(len(x)/2)))
print(x)
y = df.drop('Date', axis=1)
print(y)
plt.plot(x, y)
plt.legend(df.columns.values[1:], loc='lower left')
title = "Total Cumulative Stock Return of:\n{}\nfor period {} to {}".format(', '.join(y.columns.values.tolist()), df['Date'].iloc[0], df['Date'].iloc[len(df)-1])
plt.title(title)
plt.show()