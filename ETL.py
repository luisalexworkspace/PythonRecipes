# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import glob
import pandas as pd
from datetime import datetime

columns=['Name','Market Cap (US$ Billion)']

#Function to extract json_data

def extract_from_json(file_to_process):
    extracted_json_data = pd.read_json(file_to_process)
    return extracted_json_data

def extract():
    file_to_process = 'bank_market_cap_1.json'
    extracted_data = extract_from_json(file_to_process)
    return extracted_data

#Function to extract csv_data

def extract_from_csv(file_to_process):
    rates_dataframe = pd.read_csv(file_to_process, index_col=0)
    exchange_rate=rates_dataframe.loc['GBP', 'Rates']
    return rates_dataframe
    
#Transform function
def transform(data):
    # Write your code here
    #Change rate from USD to GBP
    data['Market Cap (US$ Billion)']=data['Market Cap (US$ Billion)']*0.732398
    data.columns=['Name','Market Cap (GBP£ Billion)']
    data['Market Cap (GBP£ Billion)']=data['Market Cap (GBP£ Billion)'].round(decimals = 2)
    return data

#Load function
def load(data_to_load):
    data_to_load.to_csv('bank_market_cap_gbp.csv', index='false')

#Log function
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')

log("ETL Job Started")
log("Extract phase Started")
extracted_data = extract()

log("Extract phase Ended")
extracted_data.head(5)
log("Transform phase Started")

transformed_data = transform(extracted_data)
log("Transform phase Ended")
print(transformed_data)

log("Load phase Started")
load(transformed_data)