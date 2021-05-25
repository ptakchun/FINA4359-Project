#In[]
import pandas as pd
import numpy as np

# In[]

df = pd.read_csv("crsp.zip", compression='zip',header=0,
             parse_dates = ['date','DCLRDT','RCRDDT'],
             dtype={"PRC": np.float64,
                  "VOL": np.float64,
                  "SHROUT": np.float64,
                  "DIVAMT": np.float64,
                  "SPREAD": np.float64,
                  "RET": np.float64,
                  "RETX": np.float64,
                  "SHRCD": str,
                  "DISTCD": str,
                  "COMNAM": str,
                  "TICKER": str,
                   'CUSIP': str})

#Data Preprocessing
df = df[(df.SHRCD.isin(('10','11')))]
df = df[(df.date <= '2011-12-31')]

df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df.PRC = df.PRC.abs()

df = df[ df['DISTCD'].str.startswith('12', na = True) ]

df.sort_values(by=['CUSIP','date'], ascending=True, inplace=True)
df = df.groupby(by=['CUSIP','date']).agg({
     'DCLRDT': 'last',
     'RCRDDT': 'last',
     'DISTCD': 'last',
     'DIVAMT': 'sum',
     'RET':'last',
     'RETX':'last',
     'VOL': 'last',
     'PRC': 'last',
     'SPREAD': 'last',
     'SHROUT': 'last',
     'year':'last',
     'month': 'last'}).reset_index()


# %%

#check to see if there was dividend payment i months ago
for i in range(1,13):
     df['divamt_' + str(i) + '_month_ago'] = df.groupby('CUSIP').DIVAMT.apply(lambda x: x.shift(i))
     df['distcd_' + str(i) + '_month_ago'] = df.groupby('CUSIP').DISTCD.apply(lambda x: x.shift(i))

# %%

#Copy data into 12 dataframes, Each represents the data 
#with dividends paid i months ago
for i in range(1,13):
     exec('df_' + str(i) + '_ago = df[ (df[\'divamt_' + str(i) + '_month_ago\'].isna() == False) & (df[\'divamt_' + str(i) + '_month_ago\'] != 0) & (df[\'RET\'].isna() == False)]')

# %%
print('Mean of monthly returns (Given Dividend Payment N Months Ago)')
for i in range(1,13):
    exec('mean_ret = pd.to_numeric(df_' + str(i) + '_ago[\'RET\'], errors=\'coerce\').mean() * 100')
    print('N = ' + str(i) + ': ' + str(round(mean_ret,2)))

# %%
print('SD of monthly returns (Given Dividend Payment N Months Ago)')
for i in range(1,13):
    exec('sd_ret = pd.to_numeric(df_' + str(i) + '_ago[\'RET\'], errors=\'coerce\').std() * 100')
    print('N = ' + str(i) + ': ' + str(round(sd_ret,2)))

# %%
for i in range(1,13):
     exec('prob = (df_' + str(i) + '_ago[\'DIVAMT\'] > 0).sum()/df_' + str(i) + '_ago[\'DIVAMT\'].count()')
     print('N = ' + str(i) + ': ' + str(round(prob,3)))

# %%
for i in range(1,13):
     exec('df_' + str(i) + '_Q_div = df_' + str(i) + '_ago[df_' + str(i) + '_ago.distcd_'+ str(i) + '_month_ago.astype(str).str[2].isin([\'0\',\'1\',\'3\',\'4\',\'5\'])]')
     exec('prob_2 = (df_' + str(i) + '_Q_div.DIVAMT > 0).sum() / df_' + str(i) + '_Q_div.DIVAMT.count()')
     print('N = ' + str(i) + ': ' + str(round(prob_2,3)))

# %%

# Panel B
def categorize(row):
     if row['divamt_3_month_ago'] > 0 and (str(row['distcd_3_month_ago'])[0:3] == '120' or str(row['distcd_3_month_ago'])[0:3] == '121' or str(row['distcd_3_month_ago'])[0:3] == '123'):
          return 1
     elif row['divamt_6_month_ago'] > 0 and (str(row['distcd_6_month_ago'])[0:3] == '120' or str(row['distcd_6_month_ago'])[0:3] == '121' or str(row['distcd_6_month_ago'])[0:3] == '123'):
          return 1
     elif row['divamt_9_month_ago'] > 0 and (str(row['distcd_9_month_ago'])[0:3] == '120' or str(row['distcd_9_month_ago'])[0:3] == '121' or str(row['distcd_9_month_ago'])[0:3] == '123'):
          return 1
     elif row['divamt_12_month_ago'] > 0 and (str(row['distcd_12_month_ago'])[0:3] == '120' or str(row['distcd_12_month_ago'])[0:3] == '121' or str(row['distcd_12_month_ago'])[0:3] == '123'):
          return 1
     elif row['divamt_6_month_ago'] > 0 and str(row['distcd_6_month_ago'])[0:3] == '124':
          return 1
     elif row['divamt_12_month_ago'] > 0 and str(row['distcd_12_month_ago'])[0:3] == '124':
          return 1
     elif row['divamt_12_month_ago'] > 0 and str(row['distcd_12_month_ago'])[0:3] == '125':
          return 1
     else:
          cat_2 = row['divamt_1_month_ago'] > 0 or row['divamt_2_month_ago'] > 0 or row['divamt_3_month_ago'] > 0 or \
          row['divamt_4_month_ago'] > 0 or row['divamt_5_month_ago'] > 0 or row['divamt_6_month_ago'] > 0 or \
          row['divamt_7_month_ago'] > 0 or row['divamt_8_month_ago'] > 0 or row['divamt_9_month_ago'] > 0 or \
          row['divamt_10_month_ago'] > 0 or row['divamt_11_month_ago'] > 0 or row['divamt_12_month_ago'] > 0
          if cat_2:
               return 2
          else:
               return 3


df['category'] = df.apply (lambda row: categorize(row), axis=1)

# %%
df_port1 = df[df['category'] == 1].groupby('date').RET.mean(numeric_only = True)
# %%
