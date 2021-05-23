#In[]
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas.tseries.offsets import MonthEnd
import statsmodels.api as sm
from tqdm import tqdm

#In[]
crsp_df = pd.read_csv("crsp.zip", compression='zip',header=0,
             parse_dates = ['date','DCLRDT','RCRDDT'],
             dtype={"PRC": np.float64,
                  "VOL": np.float64,
                  "SHROUT": np.float64,
                  "DIVAMT": np.float64,
                  "SPREAD": np.float64,
                  "SHRCD":str,
                  "DISTCD":str,
                  "COMNAM": str,
                  "TICKER": str,
                   'CUSIP': str})
#Data Preprocessing
crsp_df = crsp_df[(crsp_df.SHRCD.isin(('10','11')))]
crsp_df = crsp_df[(crsp_df.date <= '2011-12-31')]
# cusips_DISTCD_12 = crsp_df[crsp_df.DISTCD.apply(lambda x: x[:2]=='12' if isinstance(x, str) else False)].CUSIP.unique()
# crsp_df = crsp_df[crsp_df.CUSIP.isin(cusips_DISTCD_12)]

# crsp_df =  crsp_df[( crsp_df['RET'].apply(lambda x: str(x)[-1].isdigit()) )]
# crsp_df['RET'] = crsp_df['RET'].astype('float64')

crsp_df.PRC = crsp_df.PRC.abs()

crsp_df['year'] = crsp_df['date'].dt.year
crsp_df['month'] = crsp_df['date'].dt.month
crsp_df.sort_values(by=['CUSIP','date'], ascending=True, inplace=True)
crsp_df['PRC_t-1'] = crsp_df.groupby('CUSIP')['PRC'].shift(1)
crsp_df.head()

#In[]:

#In[]
crsp_div_df = crsp_df[['date','CUSIP','DCLRDT','RCRDDT','DISTCD','DIVAMT','PRC','VOL','RET','SHROUT','SPREAD','year','month']].copy()
crsp_div_df.sort_values(by=['CUSIP','date'], ascending=True, inplace=True)
crsp_div_df = crsp_div_df.groupby(by=['CUSIP','date']).agg({
     'DCLRDT': 'last',
     'RCRDDT': 'last',
     'DISTCD': 'last',
     'DIVAMT': 'sum',
     'RET':'max',
     'VOL': 'last',
     'PRC': 'last',
     'SPREAD': 'last',
     'SHROUT': 'last'})
crsp_div_df
#In[]
from multiprocessing.dummy import Pool
from time import sleep

def foo(args):
     cusip, rcrddt = args
     rcrddt_next_year = rcrddt + pd.DateOffset(years=1)
     crsp_div_df.loc[pd.IndexSlice[cusip,rcrddt:rcrddt_next_year], 'has_DIV_past_yr'] = True

crsp_div_df['has_DIV_past_yr']=False
RCRDDT_list = crsp_div_df[crsp_div_df.DIVAMT != 0.0].index

with Pool() as pool:
     inputs = tuple(RCRDDT_list)
     length = len(inputs)
     result = list(tqdm(pool.imap(foo, inputs),total=length))
     pool.close()
     pool.terminate()
     pool.join()
# for cusip, rcrddt in tqdm(RCRDDT_list):
#      rcrddt_next_year = rcrddt + pd.DateOffset(years=1)
#      # has_DIV_past_yr = ((crsp_div_df.CUSIP==cusip)&(crsp_div_df.date>=rcrddt)&(crsp_div_df.date<=rcrddt_next_year))
#      crsp_div_df.loc[pd.IndexSlice[cusip,rcrddt:rcrddt_next_year], 'has_DIV_past_yr'] = True

#In[]
crsp_div_df.has_DIV_past_yr.sum()

# crsp_div_df.loc[pd.IndexSlice[cusip,rcrddt:rcrddt_next_year], 'has_DIV_past_yr']
#In[]
crsp_df[crsp_df.duplicated(subset=['CUSIP','date'])]