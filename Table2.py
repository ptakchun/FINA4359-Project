#In[]
import pandas as pd
import numpy as np

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
#crsp_df['month_since_last_div'] = 
crsp_df['month_since_last_div'] = crsp_df.groupby('CUSIP').DIVAMT.apply(lambda x: x.ffill().shift())