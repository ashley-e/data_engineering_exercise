# import required dependencies
import os
import time
import numpy as np
import pandas as pd
import requests
import sqlite3



# EXTRACT

file_dir = os.path.join(os.getcwd(), 'DATA.csv')
df = pd.read_csv(file_dir)

# TRANSFORM

# get domains from email
df['domain'] = df['email'].str.split('@').str[1]

# get data from ip address, select fields
url_list = 'http://ip-api.com/json/' + df['ip_address'].values + '?fields=status,message,country,regionName,city,lat,lon,query'
df2 = []
for url in url_list:
    try:
        r = requests.get(url, timeout=10)
        # sleep to stay under api request limit 
        time.sleep(1)
        json = r.json()
        print(json)
        df2.append(json)
    except requests.exceptions.Timeout:
        print("Timeout occurred")
        
# create df from json list
df2 = pd.DataFrame(df2)
df2.rename(columns={'query': 'ip_address', 'regionName': 'region'}, inplace=True)

# merge api df with original df 
df = df.merge(df2, on='ip_address', how='left')

# update ip status to reason if failed
if 'message' in df.columns:
    df['status'] = np.where(df['status'] == 'fail', df['message'], df['status'])
    df.drop('message', axis=1, inplace=True)

# check for uniqueness
if df['id'].is_unique:
    print('Records are unique.')

# strip leading and trailing whitespace
df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# check for missing data
if df.isna().any().sum():
    percent = (100 * df['status'].value_counts(normalize=True).success)
    reason = df['status'].dropna().unique()
    print(str(percent) + '% of ip-api records are available.')
else:
    print('No missing data.')

# drop any duplicates
if df.duplicated().any():
    df.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)
    print('Duplicates removed.')
else: 
    print('No duplicates found.')

# LOAD

# create csv output
df.to_csv('data_results.csv', encoding='utf-8', index=False)

# load to db
con = sqlite3.connect("data.sqlite")
df.to_sql("data_results.sqlite", con, if_exists="replace")
