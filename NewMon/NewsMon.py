'''

'''
#In[]
# !pip install clickhouse-driver
from clickhouse_driver import Client
import pandas as pd
client_taq = Client.from_url('clickhouse://student_taq:thanksalan@147.8.113.184:9000/taq')
client_taq2 = Client.from_url('clickhouse://student_taq:thanksalan@147.8.113.184:9000/taq2')
client_rpna = Client.from_url('clickhouse://student_taq:thanksalan@147.8.113.184:9000/rpna')
client_writeable = Client.from_url('clickhouse://student_taq:thanksalan@147.8.113.184:9000/writeable')
# client_taq.execute('SHOW TABLES')

#In[]
taq_tables = client_taq.query_dataframe('show tables')
taq2_tables = client_taq2.query_dataframe('show tables')
rpna_tables = client_rpna.query_dataframe('show tables')
print("tables for client_taq\n", taq_tables)
print("\ntables for client_taq2\n", taq2_tables)
print("\ntables for client_rpna\n", rpna_tables)

#In[]
client_taq2.query_dataframe('select count(*) from bar')

#In[]
client_taq2.query_dataframe("select count(*) from bar where date>='2000-03-01' and date<='2000-03-31' and time>='09:30:00' and time<='16:00:00' and nbo<>0 and nbb<>0 and nbo is not NULL and nbb is not NULL and symbol is not NULL and time is not NULL and date is not NULL and nbo>=nbb ")

#In[]
client_taq.query_dataframe("select min(date) from dtaq_ctm")
#In[]
client_taq2.query_dataframe('select count(DISTINCT symbol) from bar')
#In[]
t = client_taq.query_dataframe("select date,time_m,sym_root,quantile(price) from dtaq_ctm where date>='2000-03-01' and date<='2000-03-10' and time_m>='09:30:00' and time_m<='16:00:00' and price is not NULL and sym_root is not NULL and time_m is not NULL and date is not NULL group by date, time_m, sym_root")

#In[]
t