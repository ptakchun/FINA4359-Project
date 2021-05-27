#In[]
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas.tseries.offsets import MonthEnd
# import statsmodels.api as sm
from tqdm import tqdm
# from multiprocessing.dummy import Pool
import dask.dataframe as dd
pd.set_option('display.max_rows', 500)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
# pd.set_option('display.max_columns', 500)

#In[] 1,227,297,915 size, 7675it * 10000
crsp_df = pd.read_csv("crsp_d.csv",header=0,
             parse_dates = ['date','DCLRDT','RCRDDT'],
             dtype={
                  "PERMNO": str,
                  "PRC": np.float64,
                  "VOL": np.float64,
                  "SHROUT": np.float64,
                  "DIVAMT": np.float64,
                  "SHRCD":str,
                  "DISTCD":str,
                  "COMNAM": str,
                  "TICKER": str,
                   'CUSIP': str,
                   "BID": np.float64,
                   "ASK": np.float64}, chunksize=100000)
#Data Preprocessing
#In[]
d = {}
one = False
def insert(i,x):
     if str(x)[-1].isdigit() == False:
          return
     x = float(x)
     if i not in d:
          d[i] = [x]
     else:
          d[i].append(x)
     return

for c in tqdm(crsp_df):
     c = c[(c.SHRCD.isin(('10','11'))) ]
     c = c.groupby(by=['CUSIP','date']).agg({
          'DCLRDT': 'last',
          'RCRDDT': 'last',
          'DISTCD': 'last',
          'DIVAMT': 'sum',
          'RET':'last',
          'RETX':'last',
          'PRC': 'last'}).reset_index()
     if not one:
          cusip = c.head(1).reset_index().iloc[0].CUSIP
          one = True
     count = 0
     # c['dt'] = 0
     lastDivDate = None
     for row in c.itertuples():
          if row.CUSIP != cusip:
               cusip = row.CUSIP
               # row.dt = 0
               count = 0
               lastDivDate = None
          else:
               if pd.isna(row.RCRDDT) == False:
                    lastDivDate = True
                    count = 0
                    insert(count, row.RET)
                    count += 1
               else:
                    if lastDivDate is None:
                         # row.dt = 0
                         count = 0
                    else:
                         # row.dt = count
                         insert(count, row.RET)
                         count += 1

#In[]
crsp_rev_df = pd.read_csv("crsp_d_rev2.csv",header=0,
             parse_dates = ['date','DCLRDT','RCRDDT'],
             dtype={
                  "PERMNO": str,
                  "PRC": np.float64,
                  "VOL": np.float64,
                  "SHROUT": np.float64,
                  "DIVAMT": np.float64,
                  "SHRCD":str,
                  "DISTCD":str,
                  "COMNAM": str,
                  "TICKER": str,
                   'CUSIP': str,
                   "BID": np.float64,
                   "ASK": np.float64}, chunksize=100000)
one = False
for c in tqdm(crsp_rev_df):
     c = c[(c.SHRCD.isin(('10','11'))) ]
     c = c.groupby(by=['CUSIP','date']).agg({
          'DCLRDT': 'last',
          'RCRDDT': 'last',
          'DISTCD': 'last',
          'DIVAMT': 'sum',
          'RET':'last',
          'RETX':'last',
          'PRC': 'last'}).reset_index()
     if not one:
          cusip = c.head(1).reset_index().iloc[0].CUSIP
          one = True
     count = 0
     # c['dt'] = 0
     lastDivDate = None
     for row in c.itertuples():
          if row.CUSIP != cusip:
               cusip = row.CUSIP
               # row.dt = 0
               count = 0
               lastDivDate = None
          else:
               if pd.isna(row.RCRDDT) == False:
                    lastDivDate = True
                    count = 0
                    # insert(count, row.RET)
                    count += 1
               else:
                    if lastDivDate is None:
                         # row.dt = 0
                         count = 0
                    else:
                         # row.dt = count
                         insert(count * -1 , row.RET)
                         count += 1
#In[]
print(sorted(d.keys()))
#In[]
x = range(-30, 61, 1)
dlist = [ 100*sum(d[key])/len(d[key]) for key in x ]
print(dlist)

#In[]

plt.bar(x,dlist)
plt.show()

#In[]
import pickle
with open('DIVAMT_AfterexDividendDate_avaeragePctChange2.pkl','wb') as f:
     pickle.dump(dlist, f)

#In[]
with open('DIVAMT_AfterexDividendDate_avaeragePctChange2.pkl','rb') as f:
     a = pickle.load(f)

a == dlist



#In[]
crsp_div_df = crsp_df[(crsp_df.SHRCD.isin(('10','11'))) ]

#In[]
crsp_div_df = crsp_div_df.groupby(by=['CUSIP','date']).agg({
     'DCLRDT': 'last',
     'RCRDDT': 'last',
     'DISTCD': 'last',
     'DIVAMT': 'sum',
     'RET':'last',
     'RETX':'last',
     'PRC': 'last'})


#In[]
crsp_div_df['dt'] = None
crsp_div_df['dt'] = crsp_div_df[crsp_div_df.DIVAMT != 0.0]['DISTCD'].apply(lambda x: 0 if x is not None and x[:2]=='12' else None)
g = crsp_div_df[['CUSIP','dt']].groupby(by=['CUSIP'])['dt']

#In[]
x = g.fillna(method='ffill')
y = g.isna().cumsum()
crsp_div_df['dt'] =  x + y


#In[]
crsp_div_df = crsp_div_df.compute()

#In[]
crsp_div_df.PRC = crsp_div_df.PRC.abs()
crsp_div_df['PRC_t-1'] = crsp_div_df.groupby('CUSIP')['PRC'].shift(1)
crsp_div_df.head()

#In[]
crsp_df_dd = dd.read_csv("crsp_d.csv",header=0,
             parse_dates = ['date','DCLRDT','RCRDDT'],
             dtype={
                  "PERMNO": str,
                  "PRC": np.float64,
                  "VOL": np.float64,
                  "SHROUT": np.float64,
                  "DIVAMT": np.float64,
                  "SHRCD":str,
                  "DISTCD":str,
                  "COMNAM": str,
                  "TICKER": str,
                   'CUSIP': str,
                   "BID": np.float64,
                   "ASK": np.float64})

s = crsp_df_dd.size.compute()
print('size:', s)

#In[]
pd.isna('0')