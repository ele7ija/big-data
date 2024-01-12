# Downloads data from all publicly traded stocks mostly containing starting and
# closing value for each day.
#
# This is a customized script taken from:
# https://www.kaggle.com/code/jacksoncrow/download-nasdaq-historical-data/notebook.

offset = 0
limit = 0
period = 'max' # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max

import pandas as pd

data = pd.read_csv("http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt", sep='|')
data_clean = data[data['Test Issue'] == 'N']
symbols = data_clean['NASDAQ Symbol'].tolist()
print('total number of symbols traded = {}'.format(len(symbols)))

import yfinance as yf

limit = limit if limit else len(symbols)
end = min(offset + limit, len(symbols))
is_valid = [False] * len(symbols)
for i in range(offset, end):
    s = symbols[i]
    print('Symbol: {}'.format(s))
    try:
        data = yf.download(s, period=period, threads=False)
        if len(data.index) == 0:
            continue
    
        is_valid[i] = True
        data.to_csv('data/stocks/{}.csv'.format(s))
    except Exception as e:
        print('\nFailed to download symbol {} because of: {}\n'.format(s, e))

print('Total number of valid symbols downloaded = {}'.format(sum(is_valid)))