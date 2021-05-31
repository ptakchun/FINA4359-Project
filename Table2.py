#In[]
import pandas as pd
import numpy as np

# In[]
# Import Data

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

#To aggregate the dividend amount as there might be more than one row for each month-company combination
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

#Filter out by the criteria on the paper
df = df[(df['PRC_t-1'] >= 5)]
df = df[(df.SHRCD.isin(('10','11')))]
df = df[(df.date <= '2011-12-31')]

# %%

#Copy data into 12 dataframes, Each represents the data 
#with dividends paid i months ago
for i in range(1,13):
     exec('df_' + str(i) + '_ago = df[ (df[\'divamt_' + str(i) + '_month_ago\'].isna() == False) & (df[\'divamt_' + str(i) + '_month_ago\'] != 0) & (df[\'RET\'].isna() == False)]')

# %%

#constrct dataframes that contain all dividend data
for i in range(1,13):
     exec('df_' + str(i) + '_Q_div = df_' + str(i) + '_ago[df_' + str(i) + '_ago.distcd_'+ str(i) + '_month_ago.astype(str).str[2].isin([\'0\',\'1\',\'3\',\'4\',\'5\'])]')

panel_a = {
     "Mean Return":{
          "1": round(pd.to_numeric(df_1_ago['RET'], errors='coerce').mean() * 100, 2),
          "2": round(pd.to_numeric(df_2_ago['RET'], errors='coerce').mean() * 100, 2),
          "3": round(pd.to_numeric(df_3_ago['RET'], errors='coerce').mean() * 100, 2),
          "4": round(pd.to_numeric(df_4_ago['RET'], errors='coerce').mean() * 100, 2),
          "5": round(pd.to_numeric(df_5_ago['RET'], errors='coerce').mean() * 100, 2),
          "6": round(pd.to_numeric(df_6_ago['RET'], errors='coerce').mean() * 100, 2),
          "7": round(pd.to_numeric(df_7_ago['RET'], errors='coerce').mean() * 100, 2),
          "8": round(pd.to_numeric(df_8_ago['RET'], errors='coerce').mean() * 100, 2),
          "9": round(pd.to_numeric(df_9_ago['RET'], errors='coerce').mean() * 100, 2),
          "10": round(pd.to_numeric(df_10_ago['RET'], errors='coerce').mean() * 100, 2),
          "11": round(pd.to_numeric(df_11_ago['RET'], errors='coerce').mean() * 100, 2),
          "12": round(pd.to_numeric(df_12_ago['RET'], errors='coerce').mean() * 100, 2)
     },
     "Std. Deviation":{
          "1": round(pd.to_numeric(df_1_ago['RET'], errors='coerce').std() * 100, 2),
          "2": round(pd.to_numeric(df_2_ago['RET'], errors='coerce').std() * 100, 2),
          "3": round(pd.to_numeric(df_3_ago['RET'], errors='coerce').std() * 100, 2),
          "4": round(pd.to_numeric(df_4_ago['RET'], errors='coerce').std() * 100, 2),
          "5": round(pd.to_numeric(df_5_ago['RET'], errors='coerce').std() * 100, 2),
          "6": round(pd.to_numeric(df_6_ago['RET'], errors='coerce').std() * 100, 2),
          "7": round(pd.to_numeric(df_7_ago['RET'], errors='coerce').std() * 100, 2),
          "8": round(pd.to_numeric(df_8_ago['RET'], errors='coerce').std() * 100, 2),
          "9": round(pd.to_numeric(df_9_ago['RET'], errors='coerce').std() * 100, 2),
          "10": round(pd.to_numeric(df_10_ago['RET'], errors='coerce').std() * 100, 2),
          "11": round(pd.to_numeric(df_11_ago['RET'], errors='coerce').std() * 100, 2),
          "12": round(pd.to_numeric(df_12_ago['RET'], errors='coerce').std() * 100, 2)
     },
     "All Dividends":{
          "1": round((df_1_ago['DIVAMT'] > 0).sum() / df_1_ago['DIVAMT'].count(), 2),
          "2": round((df_2_ago['DIVAMT'] > 0).sum() / df_2_ago['DIVAMT'].count(), 2),
          "3": round((df_3_ago['DIVAMT'] > 0).sum() / df_3_ago['DIVAMT'].count(), 2),
          "4": round((df_4_ago['DIVAMT'] > 0).sum() / df_4_ago['DIVAMT'].count(), 2),
          "5": round((df_5_ago['DIVAMT'] > 0).sum() / df_5_ago['DIVAMT'].count(), 2),
          "6": round((df_6_ago['DIVAMT'] > 0).sum() / df_6_ago['DIVAMT'].count(), 2),
          "7": round((df_7_ago['DIVAMT'] > 0).sum() / df_7_ago['DIVAMT'].count(), 2),
          "8": round((df_8_ago['DIVAMT'] > 0).sum() / df_8_ago['DIVAMT'].count(), 2),
          "9": round((df_9_ago['DIVAMT'] > 0).sum() / df_9_ago['DIVAMT'].count(), 2),
          "10": round((df_10_ago['DIVAMT'] > 0).sum() / df_10_ago['DIVAMT'].count(), 2),
          "11": round((df_11_ago['DIVAMT'] > 0).sum() / df_11_ago['DIVAMT'].count(), 2),
          "12": round((df_12_ago['DIVAMT'] > 0).sum() / df_12_ago['DIVAMT'].count(), 2)
     },
     "Quarterly Dividends":{
          "1": round((df_1_Q_div.DIVAMT > 0).sum() / df_1_Q_div.DIVAMT.count(), 2),
          "2": round((df_2_Q_div.DIVAMT > 0).sum() / df_2_Q_div.DIVAMT.count(), 2),
          "3": round((df_3_Q_div.DIVAMT > 0).sum() / df_3_Q_div.DIVAMT.count(), 2),
          "4": round((df_4_Q_div.DIVAMT > 0).sum() / df_4_Q_div.DIVAMT.count(), 2),
          "5": round((df_5_Q_div.DIVAMT > 0).sum() / df_5_Q_div.DIVAMT.count(), 2),
          "6": round((df_6_Q_div.DIVAMT > 0).sum() / df_6_Q_div.DIVAMT.count(), 2),
          "7": round((df_7_Q_div.DIVAMT > 0).sum() / df_7_Q_div.DIVAMT.count(), 2),
          "8": round((df_8_Q_div.DIVAMT > 0).sum() / df_8_Q_div.DIVAMT.count(), 2),
          "9": round((df_9_Q_div.DIVAMT > 0).sum() / df_9_Q_div.DIVAMT.count(), 2),
          "10": round((df_10_Q_div.DIVAMT > 0).sum() / df_10_Q_div.DIVAMT.count(), 2),
          "11": round((df_11_Q_div.DIVAMT > 0).sum() / df_11_Q_div.DIVAMT.count(), 2),
          "12": round((df_12_Q_div.DIVAMT > 0).sum() / df_12_Q_div.DIVAMT.count(), 2)
     }
}

print('Panel A - Raw Returns and Dividend Payments')
print('Returns in Current Month Given Dividend Payment N Months Ago')
print(pd.DataFrame(panel_a))

# %%

# Panel B categorize function (find out they are 1, 2 or 3)
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
     elif str(row['DISTCD'])[3:3] != '1':
          cat_2 = row['divamt_1_month_ago'] > 0 or row['divamt_2_month_ago'] > 0 or row['divamt_3_month_ago'] > 0 or \
          row['divamt_4_month_ago'] > 0 or row['divamt_5_month_ago'] > 0 or row['divamt_6_month_ago'] > 0 or \
          row['divamt_7_month_ago'] > 0 or row['divamt_8_month_ago'] > 0 or row['divamt_9_month_ago'] > 0 or \
          row['divamt_10_month_ago'] > 0 or row['divamt_11_month_ago'] > 0 or row['divamt_12_month_ago'] > 0
          if cat_2:
               return 2
          else:
               return 3
     else:
          return 4   #not categorzied

df['category'] = df.apply (lambda row: categorize(row), axis=1)

# %%
# Categorize and print Panel B
df_port1 = df[df['category'] == 1].groupby('date').RET.mean(numeric_only = True)
df_port2 = df[df['category'] == 2].groupby('date').RET.mean(numeric_only = True)
df_port3 = df[df['category'] == 3].groupby('date').RET.mean(numeric_only = True)

df_L1S2 = df_port1 - df_port2
df_l1S3 = df_port1 - df_port3

panel_b = {
     "Mean Return":{
          "[1] Predicted Dividend Month": round(df_port1.mean() * 100,2),
          "[2] All Other Companies with a Dividend in the Last 12 Months": round(df_port2.mean() * 100,2),
          "[3] All Other Companies with NO Dividend in the Last 12 Months": round(df_port3.mean() * 100,2),
          "Portfolio Long [1] and Short [2]": round(df_L1S2.mean() * 100,2),
          "Portfolio Long [1] and Short [3]": round(df_l1S3.mean() * 100,2)
     },
     "Std. Deviation":{
          "[1] Predicted Dividend Month": round(df_port1.std() * 100,2),
          "[2] All Other Companies with a Dividend in the Last 12 Months": round(df_port2.std() * 100,2),
          "[3] All Other Companies with NO Dividend in the Last 12 Months": round(df_port3.std() * 100,2),
          "Portfolio Long [1] and Short [2]": round(df_L1S2.std() * 100,2),
          "Portfolio Long [1] and Short [3]":round(df_l1S3.std() * 100,2)
     },
     "Sharpe Ratio":{
          "[1] Predicted Dividend Month": round((df_port1.mean() - 0.0029) / df_port1.std(),2),
          "[2] All Other Companies with a Dividend in the Last 12 Months": round((df_port2.mean() - 0.0029) / df_port2.std(),2),
          "[3] All Other Companies with NO Dividend in the Last 12 Months": round((df_port3.mean() - 0.0029) / df_port3.std(),2),
          "Portfolio Long [1] and Short [2]": round((df_L1S2.mean() - 0.0029) / df_L1S2.std(),2),
          "Portfolio Long [1] and Short [3]":round((df_l1S3.mean() - 0.0029) / df_l1S3.std(),2)
     },
     "1%":{
          "[1] Predicted Dividend Month": round(df_port1.quantile(0.01) * 100,2),
          "[2] All Other Companies with a Dividend in the Last 12 Months": round(df_port2.quantile(0.01) * 100,2),
          "[3] All Other Companies with NO Dividend in the Last 12 Months": round(df_port3.quantile(0.01) * 100,2),
          "Portfolio Long [1] and Short [2]": round(df_L1S2.quantile(0.01) * 100,2),
          "Portfolio Long [1] and Short [3]": round(df_l1S3.quantile(0.01) * 100,2)
     },
     "5%":{
          "[1] Predicted Dividend Month": round(df_port1.quantile(0.05) * 100,2),
          "[2] All Other Companies with a Dividend in the Last 12 Months": round(df_port2.quantile(0.05) * 100,2),
          "[3] All Other Companies with NO Dividend in the Last 12 Months": round(df_port3.quantile(0.05) * 100,2),
          "Portfolio Long [1] and Short [2]": round(df_L1S2.quantile(0.05) * 100,2),
          "Portfolio Long [1] and Short [3]": round(df_l1S3.quantile(0.05) * 100,2)
     },
     "25%":{
          "[1] Predicted Dividend Month": round(df_port1.quantile(0.25) * 100,2),
          "[2] All Other Companies with a Dividend in the Last 12 Months": round(df_port2.quantile(0.25) * 100,2),
          "[3] All Other Companies with NO Dividend in the Last 12 Months": round(df_port3.quantile(0.25) * 100,2),
          "Portfolio Long [1] and Short [2]": round(df_L1S2.quantile(0.25) * 100,2),
          "Portfolio Long [1] and Short [3]": round(df_l1S3.quantile(0.25) * 100,2)
     },
     "Median":{
          "[1] Predicted Dividend Month": round(df_port1.quantile(0.5) * 100,2),
          "[2] All Other Companies with a Dividend in the Last 12 Months": round(df_port2.quantile(0.5) * 100,2),
          "[3] All Other Companies with NO Dividend in the Last 12 Months": round(df_port3.quantile(0.5) * 100,2),
          "Portfolio Long [1] and Short [2]": round(df_L1S2.quantile(0.5) * 100,2),
          "Portfolio Long [1] and Short [3]": round(df_l1S3.quantile(0.5) * 100,2)
     },
     "75%":{
          "[1] Predicted Dividend Month": round(df_port1.quantile(0.75) * 100,2),
          "[2] All Other Companies with a Dividend in the Last 12 Months": round(df_port2.quantile(0.75) * 100,2),
          "[3] All Other Companies with NO Dividend in the Last 12 Months": round(df_port3.quantile(0.75) * 100,2),
          "Portfolio Long [1] and Short [2]": round(df_L1S2.quantile(0.75) * 100,2),
          "Portfolio Long [1] and Short [3]": round(df_l1S3.quantile(0.75) * 100,2)
     },
     "95%":{
          "[1] Predicted Dividend Month": round(df_port1.quantile(0.95) * 100,2),
          "[2] All Other Companies with a Dividend in the Last 12 Months": round(df_port2.quantile(0.95) * 100,2),
          "[3] All Other Companies with NO Dividend in the Last 12 Months": round(df_port3.quantile(0.95) * 100,2),
          "Portfolio Long [1] and Short [2]": round(df_L1S2.quantile(0.95) * 100,2),
          "Portfolio Long [1] and Short [3]": round(df_l1S3.quantile(0.95) * 100,2)
     },
     "99%":{
          "[1] Predicted Dividend Month": round(df_port1.quantile(0.99) * 100,2),
          "[2] All Other Companies with a Dividend in the Last 12 Months": round(df_port2.quantile(0.99) * 100,2),
          "[3] All Other Companies with NO Dividend in the Last 12 Months": round(df_port3.quantile(0.99) * 100,2),
          "Portfolio Long [1] and Short [2]": round(df_L1S2.quantile(0.99) * 100,2),
          "Portfolio Long [1] and Short [3]": round(df_l1S3.quantile(0.99) * 100,2)
     }
}

print('Panel B - Returns Based on Predicted Dividends')
print(pd.DataFrame(panel_b))

# %%