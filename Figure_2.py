#In[]
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
from pandas.tseries.offsets import MonthEnd
# import statsmodels.api as sm
from tqdm import tqdm
# from multiprocessing.dummy import Pool
import dask.dataframe as dd
pd.set_option('display.max_rows', 500)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
# pd.set_option('display.max_columns', 500)

#In[]
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
                   "ASK": np.float64})
#Data Preprocessing
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
crsp_div_df.PRC = crsp_div_df.PRC.abs()
crsp_div_df['PRC_t-1'] = crsp_div_df.groupby('CUSIP')['PRC'].shift(1)
crsp_div_df.head()
# crsp_df['RET'] = crsp_df['RET'].astype('float64')

#In[]
crsp_div_df
# crsp_df

#In[]
crsp_div_df['dt'] = None
crsp_div_df['dt'] = crsp_div_df[crsp_div_df.DIVAMT != 0.0]['DISTCD'].apply(lambda x: 0 if x is not None and x[:2]=='12' else None)
g = crsp_div_df.groupby(by=['CUSIP'])['dt']
crsp_div_df['dt'] = g.ffill() + g.isna().cumsum()

#In[]
crsp_div_df[:400]
