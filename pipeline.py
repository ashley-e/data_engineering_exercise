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
for url in url_list:
    try:
        r = requests.get(url, timeout=10)
        # sleep to stay under requests limit 
        time.sleep(1)
        print(r)
        json = r.json()
        df2.append(json)
    except requests.exceptions.Timeout:
        print("Timeout occurred")
        

# create df from json list
df2 = pd.DataFrame(df2)
print(df2)



# load
# print(df)
