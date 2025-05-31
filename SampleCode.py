# -*- coding: utf-8 -*-
"""
Refactored on May 11, 2025
Original Author: Dotpe
Updated for relative paths and portability
"""


import os
import pandas as pd
import datetime
from datetime import date, timedelta
import requests
import json
import numpy as np
import re
import time

# Define base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define supporting file paths
PINCODE_FILE = os.path.join(BASE_DIR, 'data', 'pincode.csv')
DND_FILE = os.path.join(BASE_DIR, 'data', 'DND.csv')
PINCODE_FILTER_FILE = os.path.join(BASE_DIR, 'LendingPlate', 'Pincode.csv')

# Configuration variables
BBuckets = ['Bucket1.csv', 'Bucket2.csv', 'Bucket3.csv', 'Bucket4.csv', 'Bulk.csv']
BBucketsSlab = [1, 2, 3, 7, 10]

hhh = 0
TT = 4
kk = 0
kk1 = kk + 1
l00 = 00000
l01 = l00 + 600000000

DD = str((date.today() - timedelta(days=hhh))).replace("-", "")

print('All Read Done')
# Define TT-based source and destination folders
DATA_ROOT = os.path.join(BASE_DIR, 'data')
data_sources = {
    0: ('KreditBee/Data/01. BData', 'LendingPlate/ApprovedData/PS'),
    1: ('KreditBee/Bulk/No BNPL', 'LendingPlate/ApprovedData/NoBNPL'),
    2: ('MP/Data', 'LendingPlate/ApprovedData/MP'),
    3: ('KreditBee/Data/0. Shub/shub/Data', 'KreditBee/Data/0. Shub/shub/LendingPlate'),
    4: ('KreditBee/Data/0. Shub/John/Data', 'KreditBee/Data/0. Shub/Jhon/LendingPlate'),
    5: ('KreditBee/Data/02. DigitalAdd/Output', 'KreditBee/Data/02. DigitalAdd/Approved Data/LendingPlate'),
}

if TT == 3:
    DD  = ''
elif TT == 4:
    NUM  = 91
    DD = f'{NUM}. Batch {NUM}_1'
    
print(TT)
print(DD)

FileName  = data_sources[TT][0] + DD +'.csv'
savelocation  = data_sources[TT][1] 
print(FileName)
print(savelocation)


# API URLs and headers
url = "https://lms.lendingplate.co.in/api/Api/affiliateApi/checkmobile"
url_LP = "https://lms.lendingplate.co.in/api/Api/affiliateApi/loanprocess"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {os.getenv('API_TOKEN', 'PLACEHOLDER_TOKEN')}",
    "Cookie": "ci_session=2imken2lq7l4f1jj7ns4id9rejq9asfm"
}

# Helper functions
def calculate_age(birth_date):
    today = datetime.date.today()
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

print('procssing Done ')


# Processing block
for hhh in range(kk, kk1):
    

    # Resolve file and save paths
    source_folder, dest_folder = data_sources[TT]
    # FileName = os.path.join(DATA_ROOT, source_folder, f"{DD}.csv")
    # savelocation = os.path.join(DATA_ROOT, dest_folder)

    os.makedirs(os.path.join(savelocation, DD), exist_ok=True)

    df = pd.read_csv(FileName)
    print('df read')
    if TT in [2, 3, 4, 5]:
        STATE = pd.read_csv(PINCODE_FILE)[['pincode', 'state_name', 'city']]
        STATE.columns = ['E10', 'city', 'state']
        df = df.merge(STATE, on='E10', how='left')
        df['Master User ID'] = ''
        df['User Status'] = ''
        df['Current Application Status'] = ''
        df['Lps At'] = ''
        df['Submitted At'] = ''
        df['Last Assessed At'] = ''
        df['Partner ID'] = ''

        df = df[['Master User ID','User Status','E4','E5','E2','E6','E7','E8','E3',
                 'Current Application Status','E11','E27','E9','E22','E23','E10',
                 'city','state','Lps At','Submitted At','Last Assessed At']]

    df.columns = ["Master User ID", "User Status", "First Name", "Last Name", "Phone", "Gender",
                  "Date Of Birth", "Pan", "Email ID", "Current Application Status",
                  "Monthly Income", "Employer Name", "Employment Type", "Address Line 1",
                  "Address Line 2", "Pincode", "City", "State", "Lps At", "Submitted At",
                  "Last Assessed At"]

    df = df[df['Email ID'].notna() & df['Monthly Income'].notna()]
    df['Monthly Income'] = df['Monthly Income'].astype(int)

    # DND Filter
    DND = pd.read_csv(DND_FILE, dtype={'Phone': str})
    DND.columns = ['Phone', 'Flag']
    df['Phone'] = df['Phone'].astype(str)
    df = df.merge(DND, on='Phone', how='left')
    df = df.loc[(df.Flag != 1) & (df.Flag != 0)]
    df = df[~df['Email ID'].str.contains('india')]
    df.drop('Flag', axis=1, inplace=True)

    # Valid Pincode Filter
    Pincode = pd.read_csv(PINCODE_FILTER_FILE)
    Pincode = Pincode[['pincode']].drop_duplicates()
    Pincode.columns = ['Pincode']
    Pincode['Flag'] = 1
    df = df.merge(Pincode, on='Pincode', how='left')
    df = df.loc[df['Flag'] == 1]
    df.drop('Flag', axis=1, inplace=True)

    # Monthly income normalization
    df['Monthly Income'] = df['Monthly Income'].apply(lambda x: min(int(x / 12), 199000) if x >= 500000 else x)
    print('Cleaniung Part 1')
    # Email cleanup and fixes (simplified)
    df['Email ID'] = df['Email ID'].str.lower()
    df['Email ID'] = df['Email ID'].str.replace(r'[^\w\.@]+', '', regex=True)
    df['Email ID'] = df['Email ID'].str.replace(r'gmil|gamil|gmai|gamail|gmial', 'gmail', regex=True)

    # Filter based on employment and income
    df = df[df['Monthly Income'] >= 20000]
    df = df[df['Employment Type'].str.lower() == 'salaried']

    # Normalize gender
    df['Gender'] = df['Gender'].replace({'f': 'Female', 'm': 'Male'})

    # Age filtering
    df['Date Of Birth'] = pd.to_datetime(df['Date Of Birth'], errors='coerce')
    df = df.dropna(subset=['Date Of Birth'])
    df['Age'] = df['Date Of Birth'].apply(calculate_age)
    df = df[(df['Age'] >= 21) & (df['Age'] <= 58)]
    df['Date Of Birth'] = df['Date Of Birth'].dt.strftime('%d/%m/%Y')

    df.drop('Age', axis=1, inplace=True)

    # Assign ref_id
    df['ref_id'] = np.random.randint(100000000, 999999999, size=df.shape[0])

    # API Fields
    df['DDStatus'] = ''
    df['DDMessage'] = ''
    df['LCStatus'] = ''
    df['LCMessage'] = ''
    df['LCReason'] = ''
    print('Cleaning Done')
    for i in range(df.shape[0]):
        print(f"Processing row {i}")

        data = {
            "mobile": str(df.iloc[i]['Phone'])[:10],
            "partner_id": "AADIFINANCE",
            "ref_id": str(df.iloc[i]['ref_id'])
        }

        try:
            response = requests.post(url, json=data, headers=headers)
            responseMobile = response.json()
            print(responseMobile)
        except:
            time.sleep(5)
            continue

        df.at[i, 'DDStatus'] = responseMobile.get("status", '')
        df.at[i, 'DDMessage'] = responseMobile.get("message", '')
        
        print(responseMobile.get("status", ''))
        
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
            
            print(responseLoan.get("Status", ''))
            
            try:
                response = requests.post(url_LP, json=loan_payload, headers=headers)
                responseLoan = response.json()
                print(responseLoan)
                df.at[i, 'LCStatus'] = responseLoan.get("Status", '')
                df.at[i, 'LCMessage'] = responseLoan.get("Message", '')
                df.at[i, 'LCReason'] = responseLoan.get("reason", '')
            except:
                time.sleep(5)

        # Save every 500 records
        if i % 500 == 0:
            print(os.path.join(savelocation, DD, f"{l01}.csv"))
            df.to_csv(os.path.join(savelocation, DD, f"{l01}.csv"), index=False)

    # Final save
    print(os.path.join(savelocation, DD, f"{l01}.csv"))
    df.to_csv(os.path.join(savelocation, DD, f"{l01}.csv"), index=False)
