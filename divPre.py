#In[]
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm

#In[]
crsp_df = pd.read_csv("crsp.zip", compression='zip',header=0,
             parse_dates = ['date','DCLRDT','RCRDDT'],
             dtype={"PRC": np.float64,
                  "VOL": np.float64,
                  "SHROUT": np.float64,
                  "DIVAMT": np.float64,
                  "SPREAD": np.float64,
                  "COMNAM": str,
                  "TICKER": str,
                   'CUSIP': str})
#Data Preprocessing
crsp_df = crsp_df[(crsp_df['PRC']>0)]
crsp_df =  crsp_df[( crsp_df['RET'].apply(lambda x: str(x)[-1].isdigit()) )]
crsp_df['RET'] = crsp_df['RET'].astype('float64')
crsp_df['year'] = crsp_df['date'].dt.year
crsp_df['month'] = crsp_df['date'].dt.month

#In[]
crsp_div_df = crsp_df.dropna(subset=['DIVAMT'])
crsp_div_df.describe()

#In[]
crsp_div_df