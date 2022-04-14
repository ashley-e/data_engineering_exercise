# import required dependencies
from ctypes import addressof
from ipaddress import ip_address
from operator import index
import os
from string import whitespace
import time
from typing import KeysView
from matplotlib import transforms
import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine
import psycopg2
import io


from yaml import load

# extract
file_dir = os.path.join(os.getcwd(), 'DATA.csv')
df = pd.read_csv(file_dir)

# transform
# get domains from email
df['domain'] = df['email'].str.split('@').str[1]

# get data from ip address
url_list = 'http://ip-api.com/json/' + df['ip_address'].values
df2 = []
for url in url_list:
    try:
        r = requests.get(url, timeout=10)
        # sleep to stay under requests limit 
        time.sleep(1)
        json = r.json()
        print(json)
        df2.append(json)
    except requests.exceptions.Timeout:
        print("Timeout occurred")
        
# create df from json list
df2 = pd.DataFrame(df2)
df2.rename(columns={'query': 'ip_address', 'regionName': 'region'}, inplace=True)
print(df2)
# merge api df with original df 
df = df.merge(df2, on='ip_address', how='left',)
print(df)

# update ip status to reason if failed
df['status'] = np.where(df['status'] == 'fail', df['message'], df['status'])

# drop extra columns
df.drop(columns=['countryCode','region','zip','timezone','as','message'], inplace=True)

# check for uniqueness in index
if df['id'].is_unique:
    print('Records are unique.')
    df.set_index('id', inplace=True)

# strip leading and trailing whitespace
df.str.strip(inplace=True)

# check for missing data
if df.isna().any().sum():
    print((100 * df.isna().any().sum() / df.any().sum()) + '%% of data is missing.')
else:
    print('No missing data.')

# drop any duplicates
if df.duplicated().any():
    df.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)
    print('Duplicates removed.')
else: 
    print('No duplicates found.')

# load
# create csv output
df.to_csv('out.csv', encoding='utf-8', index=False)
# load to postgresdb
# engine = create_engine('postgresql://postgres:postgres@localhost:5432/data_db')
# df.to_sql('data', engine, if_exists='replace',index=False)
