#In[]
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
from pandas.tseries.offsets import MonthEnd
# import statsmodels.api as sm
from tqdm import tqdm
from multiprocessing.dummy import Pool
pd.set_option('display.max_rows', 500)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
# pd.set_option('display.max_columns', 500)

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
cusips_DISTCD_12 = crsp_df[crsp_df.DISTCD.apply(lambda x: x[:2]=='12' if isinstance(x, str) else False)].CUSIP.unique()
crsp_df = crsp_df[crsp_df.CUSIP.isin(cusips_DISTCD_12)]

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
crsp_div_df = crsp_df[['date','CUSIP','DCLRDT','RCRDDT','DISTCD','DIVAMT','PRC','VOL','RET','SHROUT','SPREAD','year','month','PRC_t-1','RETX']].copy()
crsp_div_df.sort_values(by=['CUSIP','date'], ascending=True, inplace=True)
crsp_div_df = crsp_div_df.groupby(by=['CUSIP','date']).agg({
     'DCLRDT': 'last',
     'RCRDDT': 'last',
     'DISTCD': 'last',
     'DIVAMT': 'sum',
     'RET':'last',
     'RETX':'last',
     'VOL': 'last',
     'PRC': 'last',
     'PRC_t-1': 'last',
     'SPREAD': 'last',
     'SHROUT': 'last'})
crsp_div_df['MCAP'] = crsp_div_df['PRC'] * crsp_div_df['SHROUT']
crsp_div_df

#In[]
crsp_div_df.to_pickle('./crsp_div_df_afterGroupBy.pkl.zip', compression='zip')
#In[]
crsp_div_df.sort_values(by=['CUSIP','date'], ascending=True, inplace=True)
crsp_div_df['freq'] = None
crsp_div_df['freq'] = crsp_div_df[crsp_div_df.RCRDDT.isna()==False]['DISTCD'].apply(lambda x: x[2:3] if x is not None else None)
crsp_div_df['freq'] = crsp_div_df.groupby(by=['CUSIP']).fillna(method='ffill', limit=11)['freq']

#In[]
crsp_div_df.loc[:,['RCRDDT','DIVAMT','DISTCD','freq']][:400]
# crsp_div_df['DISTCD'].apply(lambda x: x[2:3] if x is not None else None)[:400]

#In[]
crsp_div_df['MCAP'] = crsp_div_df['PRC'] * crsp_div_df['SHROUT']
crsp_div_df['TURNOVER'] = crsp_div_df['VOL'] / crsp_div_df['SHROUT']

crsp_div_df['div_yield'] = crsp_div_df['DIVAMT'].apply(lambda x: 0.0 if x.isna() else x)


#In[] panel A & B
fil_A = (crsp_div_df.freq.isna() == False) & (crsp_div_df["PRC_t-1"].isna()==False) & (crsp_div_df["PRC_t-1"] >= 5) & (crsp_div_df.freq < '6')
fil_B = (crsp_div_df.freq.isna() == True) |  (crsp_div_df.freq >= '6')

for fil in [fil_A,fil_B]:
     print(crsp_div_df[fil][['MCAP','TURNOVER','SPREAD']].describe())
     print('Number of Firm Months', len(crsp_div_df[fil]))
     print('Number of Firms', len(pd.Series(crsp_div_df.index.get_level_values('CUSIP')).unique() ))

#In[] Panel C:
s = crsp_div_df[fil_A].freq
s.groupby(s).count()/1494129 * 100