#In[]
import pandas as pd
import numpy as np

# In[]
df = pd.read_pickle('crsp_div_df_afterGroupBy.pkl.zip', compression='zip')


# %%

#check to see if there was dividend payment i months ago
for i in range(1,13):
     crsp_div_df['div_' + str(i) + '_month_ago'] = crsp_div_df.groupby('CUSIP').DIVAMT.apply(lambda x: x.shift(i))

# %%
for i in range(1,13):
     exec('df_' + str(i) + '_ago = crsp_div_df.loc[crsp_div_df[\'div_'+ str(i) + '_month_ago\'] != np.nan, CUSIP, DISTCD, DIVAMT, year, month]')