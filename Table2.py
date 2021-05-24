#In[]
import pandas as pd
import numpy as np

# In[]
import sys
sys.setrecursionlimit(1000000)
df = pd.read_pickle('crsp_div_df_afterGroupBy.pkl.zip', compression='zip')
df.head()

# %%

#check to see if there was dividend payment i months ago
for i in range(1,13):
     df['div_' + str(i) + '_month_ago'] = df.groupby('CUSIP').DIVAMT.apply(lambda x: x.shift(i))

# %%

#Copy data into 12 dataframes, Each represents the data 
#with dividends paid i months ago
for i in range(1,13):
     exec('df_' + str(i) + '_ago = df[ (df[\'div_' + str(i) + '_month_ago\'].isna() == False) & (df[\'div_' + str(i) + '_month_ago\'] != 0) & (df[\'RET\'].isna() == False)]')

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
     exec('prob = (df_' + str(i) + '_ago[\'DIVAMT\'] > 0).sum()/df_' + str(i) + '_ago[\'DIVAMT\'].count()')
     print('N = ' + str(i) + ': ' + str(round(prob,3)))