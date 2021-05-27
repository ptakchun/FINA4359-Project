#In[]
import pandas as pd
import numpy as np

# In[]

na_values = ['B','C']

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
                   'CUSIP': str},
             na_values = na_values)

#Data Preprocessing
df.PRC = df.PRC.abs()

df['PRC_t-1'] = df.groupby('CUSIP')['PRC'].shift(1)
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month

df.sort_values(by=['CUSIP','date'], ascending=True, inplace=True)
df = df.groupby(by=['CUSIP','date']).agg({
     'SHRCD': 'last',
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
     'year':'last',
     'month': 'last'}).reset_index()


# %%

#check to see if there was dividend payment i months ago
for i in range(1,13):
     df['divamt_' + str(i) + '_month_ago'] = df.groupby('CUSIP').DIVAMT.apply(lambda x: x.shift(i))
     df['distcd_' + str(i) + '_month_ago'] = df.groupby('CUSIP').DISTCD.apply(lambda x: x.shift(i))

df = df[(df['PRC_t-1'] >= 5)]
df = df[(df.SHRCD.isin(('10','11')))]
df = df[(df.date <= '2011-12-31')]

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
               if row['divamt_1_month_ago'] > 0:
                    return 4
               else:
                    return 2
          else:
               return 3

df['category'] = df.apply (lambda row: categorize(row), axis=1)

# %%
df_port1 = df[df['category'] == 1].groupby('date').RET.mean(numeric_only = True)
df_port2 = df[df['category'] == 2].groupby('date').RET.mean(numeric_only = True)
df_port3 = df[df['category'] == 3].groupby('date').RET.mean(numeric_only = True)

df_L1S2 = df_port1 - df_port2
df_l1S3 = df_port1 - df_port3

print('Table II Panel B')
print(round(df_port1.mean() * 100,2), round(df_port1.std() * 100,2), round(df_port1.quantile(0.01) * 100,2), round(df_port1.quantile(0.05) * 100,2), round(df_port1.quantile(0.25) * 100,2), round(df_port1.quantile(0.5) * 100,2), round(df_port1.quantile(0.75) * 100,2), round(df_port1.quantile(0.95) * 100,2), round(df_port1.quantile(0.99) * 100,2))
print(round(df_port2.mean() * 100,2), round(df_port2.std() * 100,2), round(df_port2.quantile(0.01) * 100,2), round(df_port2.quantile(0.05) * 100,2), round(df_port2.quantile(0.25) * 100,2), round(df_port2.quantile(0.5) * 100,2), round(df_port2.quantile(0.75) * 100,2), round(df_port2.quantile(0.95) * 100,2), round(df_port2.quantile(0.99) * 100,2))
print(round(df_port3.mean() * 100,2), round(df_port3.std() * 100,2), round(df_port3.quantile(0.01) * 100,2), round(df_port3.quantile(0.05) * 100,2), round(df_port3.quantile(0.25) * 100,2), round(df_port3.quantile(0.5) * 100,2), round(df_port3.quantile(0.75) * 100,2), round(df_port3.quantile(0.95) * 100,2), round(df_port3.quantile(0.99) * 100,2))
print(round(df_L1S2.mean() * 100,2), round(df_L1S2.std() * 100,2), round(df_L1S2.quantile(0.01) * 100,2), round(df_L1S2.quantile(0.05) * 100,2), round(df_L1S2.quantile(0.25) * 100,2), round(df_L1S2.quantile(0.5) * 100,2), round(df_L1S2.quantile(0.75) * 100,2), round(df_L1S2.quantile(0.95) * 100,2), round(df_L1S2.quantile(0.99) * 100,2))
print(round(df_l1S3.mean() * 100,2), round(df_l1S3.std() * 100,2), round(df_l1S3.quantile(0.01) * 100,2), round(df_l1S3.quantile(0.05) * 100,2), round(df_l1S3.quantile(0.25) * 100,2), round(df_l1S3.quantile(0.5) * 100,2), round(df_l1S3.quantile(0.75) * 100,2), round(df_l1S3.quantile(0.95) * 100,2), round(df_l1S3.quantile(0.99) * 100,2))

# %%