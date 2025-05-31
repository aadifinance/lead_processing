# -*- coding: utf-8 -*-
"""
Refactored on May 31, 2025
Original Author: Dotpe
Batch processing with low memory usage, garbage collection, and memory usage monitoring.
"""

import os
import pandas as pd
import datetime
from datetime import date, timedelta
import requests
import numpy as np
import re
import time
import gc
import psutil  # To monitor memory usage

# Define base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
print("Step 1: BASE_DIR defined.")

# Define supporting file paths
PINCODE_FILE = os.path.join(BASE_DIR, 'data', 'pincode.csv')
DND_FILE = os.path.join(BASE_DIR, 'data', 'DND.csv')
PINCODE_FILTER_FILE = os.path.join(BASE_DIR, 'LendingPlate', 'Pincode.csv')
print("Step 2: Supporting file paths set.")

# Configuration variables
TT = 4
hhh = 0
l00 = 00000
l01 = l00 + 600000000

DD = str((date.today() - timedelta(days=hhh))).replace("-", "")
print(f"Step 3: DD calculated as {DD}.")

data_sources = {
    0: ('data/KreditBee/Data/01. BData', 'LendingPlate/ApprovedData/PS'),
    1: ('data/KreditBee/Bulk/No BNPL', 'LendingPlate/ApprovedData/NoBNPL'),
    2: ('MP/Data', 'LendingPlate/ApprovedData/MP'),
    3: ('data/KreditBee/Data/0. Shub/shub/Data', 'KreditBee/Data/0. Shub/shub/LendingPlate'),
    4: ('data/KreditBee/Data/0. Shub/John/Data', 'KreditBee/Data/0. Shub/Jhon/LendingPlate'),
    5: ('data/KreditBee/Data/02. DigitalAdd/Output', 'KreditBee/Data/02. DigitalAdd/Approved Data/LendingPlate'),
}
print("Step 4: Data source dictionary prepared.")

if TT == 3:
    DD = ''
elif TT == 4:
    NUM = 90
    DD = f'{NUM}. Batch {NUM}_1'
print(f"Step 5: Final DD string is {DD}.")

FileName = os.path.join(BASE_DIR, data_sources[TT][0], f'{DD}.csv')
savelocation = os.path.join(BASE_DIR, data_sources[TT][1], DD)
os.makedirs(savelocation, exist_ok=True)
print(f"Step 6: Source file: {FileName}")
print(f"Step 7: Save location: {savelocation}")

url = "https://lms.lendingplate.co.in/api/Api/affiliateApi/checkmobile"
url_LP = "https://lms.lendingplate.co.in/api/Api/affiliateApi/loanprocess"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {os.getenv('API_TOKEN', 'PLACEHOLDER_TOKEN')}",
    "Cookie": "ci_session=2imken2lq7l4f1jj7ns4id9rejq9asfm"
}
print("Step 8: API URLs and headers defined.")

def calculate_age(birth_date):
    today = datetime.date.today()
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

print("Step 9: calculate_age function defined.")

# Load lookup tables once
print("Step 10: Loading DND data.")
DND = pd.read_csv(DND_FILE, dtype={'Phone': str})
DND.columns = ['Phone', 'Flag']

print("Step 11: Loading Pincode filter data.")
Pincode = pd.read_csv(PINCODE_FILTER_FILE)
Pincode = Pincode[['pincode']].drop_duplicates()
Pincode.columns = ['Pincode']
Pincode['Flag'] = 1

STATE = pd.read_csv(PINCODE_FILE)[['pincode', 'state_name', 'city']]
STATE.columns = ['E10', 'city', 'state']
print("Step 12: Lookup tables loaded.")

chunksize = 50
batch_number = 0
print(f"Step 13: Starting batch processing with chunksize={chunksize}.")

for chunk in pd.read_csv(FileName, chunksize=chunksize):
    batch_number += 1
    print(f"\n--- Processing Batch {batch_number} ---")
    print(f"Memory usage before chunk: {psutil.virtual_memory().percent}%")

    df = chunk.copy()
    print(f"Step 14: Batch {batch_number} data loaded with {df.shape[0]} rows.")

    if TT in [2, 3, 4, 5]:
        df = df.merge(STATE, on='E10', how='left')
        df['Master User ID'] = ''
        df['User Status'] = ''
        df['Current Application Status'] = ''
        df['Lps At'] = ''
        df['Submitted At'] = ''
        df['Last Assessed At'] = ''
        df['Partner ID'] = ''

        df = df[['Master User ID', 'User Status', 'E4', 'E5', 'E2', 'E6', 'E7', 'E8', 'E3',
                 'Current Application Status', 'E11', 'E27', 'E9', 'E22', 'E23', 'E10',
                 'city', 'state', 'Lps At', 'Submitted At', 'Last Assessed At']]
        print("Step 15: Columns aligned.")

    df.columns = ["Master User ID", "User Status", "First Name", "Last Name", "Phone", "Gender",
                  "Date Of Birth", "Pan", "Email ID", "Current Application Status",
                  "Monthly Income", "Employer Name", "Employment Type", "Address Line 1",
                  "Address Line 2", "Pincode", "City", "State", "Lps At", "Submitted At",
                  "Last Assessed At"]
    print("Step 16: Columns renamed.")

    df = df[df['Email ID'].notna() & df['Monthly Income'].notna()]
    df['Monthly Income'] = df['Monthly Income'].astype(int)

    df['Phone'] = df['Phone'].astype(str)
    df = df.merge(DND, on='Phone', how='left')
    df = df.loc[(df.Flag != 1) & (df.Flag != 0)]
    df = df[~df['Email ID'].str.contains('india', na=False)]
    df.drop('Flag', axis=1, inplace=True)

    df = df.merge(Pincode, on='Pincode', how='left')
    df = df.loc[df['Flag'] == 1]
    df.drop('Flag', axis=1, inplace=True)

    df['Monthly Income'] = df['Monthly Income'].apply(lambda x: min(int(x / 12), 199000) if x >= 500000 else x)
    df['Email ID'] = df['Email ID'].str.lower()
    df['Email ID'] = df['Email ID'].str.replace(r'[^\w\.@]+', '', regex=True)
    df['Email ID'] = df['Email ID'].str.replace(r'gmil|gamil|gmai|gamail|gmial', 'gmail', regex=True)

    df = df[df['Monthly Income'] >= 20000]
    df = df[df['Employment Type'].str.lower() == 'salaried']
    df['Gender'] = df['Gender'].replace({'f': 'Female', 'm': 'Male'})

    df['Date Of Birth'] = pd.to_datetime(df['Date Of Birth'], errors='coerce')
    df = df.dropna(subset=['Date Of Birth'])
    df['Age'] = df['Date Of Birth'].apply(calculate_age)
    df = df[(df['Age'] >= 21) & (df['Age'] <= 58)]
    df['Date Of Birth'] = df['Date Of Birth'].dt.strftime('%d/%m/%Y')
    df.drop('Age', axis=1, inplace=True)

    df['ref_id'] = np.random.randint(100000000, 999999999, size=df.shape[0])

    df['DDStatus'] = ''
    df['DDMessage'] = ''
    df['LCStatus'] = ''
    df['LCMessage'] = ''
    df['LCReason'] = ''
    print(f"Step 17: Preprocessing completed for Batch {batch_number}.")

    for i in range(df.shape[0]):
        print(f"Step 18: Row {i} API calls started.")
        data = {
            "mobile": str(df.iloc[i]['Phone'])[:10],
            "partner_id": "AADIFINANCE",
            "ref_id": str(df.iloc[i]['ref_id'])
        }

        try:
            response = requests.post(url, json=data, headers=headers)
            responseMobile = response.json()
            print(f"Step 19: checkmobile status: {responseMobile.get('status', '')}")
        except Exception as e:
            print(f"Error in checkmobile API for row {i}: {e}")
            time.sleep(5)
            continue

        df.at[i, 'DDStatus'] = responseMobile.get("status", '')
        df.at[i, 'DDMessage'] = responseMobile.get("message", '')

        if responseMobile.get("status") == "S":
            loan_payload = {
                "partner_id": "AADIFINANCE",
                "ref_id": str(df.iloc[i]['ref_id']),
                "mobile": str(df.iloc[i]['Phone'])[:10],
                "customer_name": f"{df.iloc[i]['First Name']} {df.iloc[i]['Last Name']}",
                "pancard": str(df.iloc[i]['Pan']),
                "dob": str(df.iloc[i]['Date Of Birth']),
                "pincode": str(df.iloc[i]['Pincode']),
                "profession": "SAL",
                "net_mothlyincome": str(df.iloc[i]['Monthly Income'])
            }

            try:
                response = requests.post(url_LP, json=loan_payload, headers=headers)
                responseLoan = response.json()
                print(f"Step 20: loanprocess status: {responseLoan.get('Status', '')}")
                df.at[i, 'LCStatus'] = responseLoan.get("Status", '')
                df.at[i, 'LCMessage'] = responseLoan.get("Message", '')
                df.at[i, 'LCReason'] = responseLoan.get("reason", '')
            except Exception as e:
                print(f"Error in loanprocess API for row {i}: {e}")
                time.sleep(5)

    batch_file = os.path.join(savelocation, f"{l01}_{batch_number}.csv")
    df.to_csv(batch_file, index=False)
    print(f"Step 21: Batch {batch_number} saved to {batch_file}.")

    del df
    gc.collect()
    print(f"Memory usage after batch {batch_number}: {psutil.virtual_memory().percent}%")

print("\nStep 22: All batch processing completed.")
