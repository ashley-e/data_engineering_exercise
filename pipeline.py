# import required dependencies
from ctypes import addressof
from ipaddress import ip_address
import os
from time import sleep
import time
from typing import KeysView
from matplotlib import transforms
from numpy import extract
import pandas as pd
import requests
import sqlite3

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
for url in url_list[:5]:
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
df.drop(columns=['countryCode','region','zip','timezone','as'], inplace=True)
print(df)

# load
df.to_csv('out.csv', encoding='utf-8', index=False)
