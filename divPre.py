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
# cusips_DISTCD_12 = crsp_div_df[crsp_div_df.DISTCD.apply(lambda x: x[:2]=='12' if isinstance(x, str) else False)].CUSIP.unique()
# crsp_div_df = crsp_div_df[crsp_div_df.CUSIP.isin(cusips_DISTCD_12)]
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
     'SHROUT': 'last',
     'year':'last'})
crsp_div_df['MCAP'] = crsp_div_df['PRC'] * crsp_div_df['SHROUT']
crsp_div_df['TURNOVER'] = crsp_div_df['VOL'] / crsp_div_df['SHROUT']
# crsp_div_df.to_pickle('./crsp_div_df_afterGroupBy.pkl.zip', compression='zip')
crsp_div_df
#In[] Find months with dividend past year
crsp_div_df.sort_values(by=['CUSIP','date'], ascending=True, inplace=True)
crsp_div_df['freq'] = None
# crsp_div_df['freq'] = crsp_div_df[crsp_div_df.RCRDDT.isna()==False]['DISTCD'].apply(lambda x: x[2:3] if x is not None else None)
crsp_div_df['freq'] = crsp_div_df[crsp_div_df.DIVAMT != 0.0]['DISTCD'].apply(lambda x: x[2] if x is not None and x[:2]=='12' else None)
crsp_div_df['freq'] = crsp_div_df.groupby(by=['CUSIP']).fillna(method='ffill', limit=11)['freq']

#In[]
# crsp_div_df.loc[:,['RCRDDT','DIVAMT','DISTCD','freq']][:400]
# crsp_div_df['DISTCD'].apply(lambda x: x[2:3] if x is not None else None)[:400]

#In[]
crsp_div_df['div_yield'] = crsp_div_df['DIVAMT'].apply(lambda x: 0.0 if x.isna() else x)

#In[] load compustat bkvlps and merge to crsp_div_df
compustat_df = pd.read_csv("compustat.zip", compression='zip', header=0,
     parse_dates = ['datadate'],dtype={
          'fyear':np.float64,
          'cusip': str,
          'bkvlps': np.float64
     })
compustat_df = compustat_df[(compustat_df.indfmt != 'FS') & (compustat_df.curcd=='USD')]
compustat_df.dropna(subset=['fyear','cusip'], inplace=True)
compustat_df['CUSIP'] = compustat_df['cusip'].apply(lambda x: None if pd.isna(x) else x[:-1]) 
compustat_df.fyear = compustat_df.fyear.astype(np.int64)
compustat_df['year'] = compustat_df.fyear.apply(lambda x: x+1)
cc_df = pd.merge(crsp_div_df.reset_index(), compustat_df[['CUSIP','year','bkvlps']], on=['CUSIP','year'], how='left')
cc_df.set_index(['CUSIP', 'date'], inplace=True)
cc_df['BM'] =  cc_df['bkvlps']/cc_df['PRC'] 

#In[]
cc_df

#In[] Table 1 panel A & B & C
cusips_DISTCD_12 = pd.Series(cc_df[cc_df.DISTCD.apply(lambda x: x[:2]=='12' if isinstance(x, str) else False)].index.get_level_values('CUSIP')).unique()




panel_A = cc_df.loc[pd.IndexSlice[cusips_DISTCD_12,:]]
fil_A = (panel_A["PRC_t-1"] > 5) & (panel_A.freq.isna() == False) & (panel_A.freq != '2')
print("Panel A - Firms with a Dividend in the Past Year")
print(panel_A[fil_A][['MCAP','BM','TURNOVER','SPREAD']].describe())
print('Number of Firm Months', len(panel_A[fil_A]))
print('Number of Firms', len(pd.Series(panel_A.index.get_level_values('CUSIP')).unique() ))

fil_B = ((cc_df.freq.isna() == True)) & (cc_df["PRC_t-1"] > 5)
panel_B = cc_df[fil_B]
print("Panel B - Firms with No Dividend in the Past Year")
print(panel_B[['MCAP','BM','TURNOVER','SPREAD']].describe())
print('Number of Firm Months', len(panel_B))
print('Number of Firms', len(pd.Series(panel_B.index.get_level_values('CUSIP')).unique() ))

#Panel C:

fil_C = (cc_df["PRC_t-1"] >= 5) & (cc_df.freq < '6')
s = cc_df[fil_C].freq
freq_dist = s.groupby(s).count()/ s.groupby(s).count().sum()
any_freq = s.groupby(s).count().sum()/len(cc_df[(cc_df["PRC_t-1"] >= 5)])
panel_C = {
     "Pct of Firm Months with Div pre yr":{
          "Any Freq": any_freq,
          'Monthly': freq_dist[2]*any_freq,
          'Quarterly': freq_dist[3]*any_freq,
          'Semi-Annual': freq_dist[4]*any_freq,
          'Annual': freq_dist[5]*any_freq,
          'Unknown Frequency': (freq_dist[0] + freq_dist[1])*any_freq
     },
     'Pct of Div Obs': {
          'Monthly': freq_dist[2],
          'Quarterly': freq_dist[3],
          'Semi-Annual': freq_dist[4],
          'Annual': freq_dist[5],
          'Unknown Frequency': freq_dist[0] + freq_dist[1]
          }    
}
print("\nPanel C: Distribution of Dividend Frequencies(%)")
print(pd.DataFrame(panel_C)*100)

#In[]
cusips_DISTCD_12 = pd.Series(cc_df[cc_df.DISTCD.apply(lambda x: x[:2]=='12' if isinstance(x, str) else False)].index.get_level_values('CUSIP')).unique()
cc_df.loc[pd.IndexSlice[cusips_DISTCD_12,:]]
len(cusips_DISTCD_12)

#In[]
s.groupby(s).count().sum()/len(cc_df[(cc_df["PRC_t-1"] >= 5)])
