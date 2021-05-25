#In[]
import pandas as pd
import numpy as np

# In[]
import sys
sys.setrecursionlimit(1000000)
df = pd.read_pickle('crsp_div_df_afterGroupBy.pkl.zip', compression='zip')
df = df[ df['DISTCD'].str.startswith('12', na = True) ]
df.head()

# %%

#check to see if there was dividend payment i months ago
for i in range(1,13):
     df['divamt_' + str(i) + '_month_ago'] = df.groupby('CUSIP').DIVAMT.apply(lambda x: x.shift(i))
     df['distcd_' + str(i) + '_month_ago'] = df.groupby('CUSIP').DIVAMT.apply(lambda x: x.shift(i))

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
def categorize(row):
     if row['divamt_3_month_ago'] > 0 & (row['distcd_3_month_ago'].startswith('120') | row['distcd_3_month_ago'].startswith('121') | row['distcd_3_month_ago'].startswith('123')):
          return 1
     elif row['divamt_6_month_ago'] > 0 & (row['distcd_6_month_ago'].startswith('120') | row['distcd_6_month_ago'].startswith('121') | row['distcd_6_month_ago'].startswith('123')):
          return 1
     elif row['divamt_9_month_ago'] > 0 & (row['distcd_9_month_ago'].startswith('120') | row['distcd_9_month_ago'].startswith('121') | row['distcd_9_month_ago'].startswith('123')):
          return 1
     elif row['divamt_12_month_ago'] > 0 & (row['distcd_12_month_ago'].startswith('120') | row['distcd_12_month_ago'].startswith('121') | row['distcd_12_month_ago'].startswith('123')):
          return 1
     elif row['divamt_6_month_ago'] > 0 & row['distcd_6_month_ago'].startswith('124'):
          return 1
     elif row['divamt_12_month_ago'] > 0 & row['distcd_12_month_ago'].startswith('124'):
          return 1
     elif row['divamt_12_month_ago'] > 0 & row['distcd_12_month_ago'].startswith('125'):
          return 1
     else:
          for i in range(1,13):
               exec('a = row[\'divamt_' + str(i) +'_month_ago\']')
               cat_2 = False
               if a > 0:
                    cat_2 = True
                    break
          
          if cat_2:
               return 2
          else:
               return 3
