# -*- coding: utf-8 -*-
"""
Refactored on May 31, 2025
Batch processing with very low memory use, explicit GC, memory logging,
and a fix for the Age-column dtype issue.
"""

import os, gc, time, psutil
import pandas as pd
import numpy as np
import datetime
from datetime import date, timedelta
import re, requests

# ── Paths & constants ──────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
print("Step 1: BASE_DIR =", BASE_DIR)

PINCODE_FILE        = os.path.join(BASE_DIR, 'data', 'pincode.csv')
DND_FILE            = os.path.join(BASE_DIR, 'data', 'DND.csv')
PINCODE_FILTER_FILE = os.path.join(BASE_DIR, 'LendingPlate', 'Pincode.csv')

TT  = 4
hhh = 0                 # days offset for DD
l01 = 600000000         # file-name seed

DD  = str((date.today() - timedelta(days=hhh))).replace("-", "")
if TT == 3:
    DD = ''
elif TT == 4:
    NUM = 90
    DD  = f'{NUM}. Batch {NUM}_1'

data_sources = {
    0: ('data/KreditBee/Data/01. BData',                 'LendingPlate/ApprovedData/PS'),
    1: ('data/KreditBee/Bulk/No BNPL',                   'LendingPlate/ApprovedData/NoBNPL'),
    2: ('MP/Data',                                       'LendingPlate/ApprovedData/MP'),
    3: ('data/KreditBee/Data/0. Shub/shub/Data',         'KreditBee/Data/0. Shub/shub/LendingPlate'),
    4: ('data/KreditBee/Data/0. Shub/John/Data',         'KreditBee/Data/0. Shub/Jhon/LendingPlate'),
    5: ('data/KreditBee/Data/02. DigitalAdd/Output',     'KreditBee/Data/02. DigitalAdd/Approved Data/LendingPlate'),
}

SRC_FILE   = os.path.join(BASE_DIR, data_sources[TT][0], f'{DD}.csv')
SAVE_DIR   = os.path.join(BASE_DIR, data_sources[TT][1], DD)
os.makedirs(SAVE_DIR, exist_ok=True)

print("Step 2: Source  =", SRC_FILE)
print("Step 3: Output  =", SAVE_DIR)

# ── API setup ──────────────────────────────────────────────────────────────────
URL_CHECK = "https://lms.lendingplate.co.in/api/Api/affiliateApi/checkmobile"
URL_LOAN  = "https://lms.lendingplate.co.in/api/Api/affiliateApi/loanprocess"
HEADERS   = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {os.getenv('API_TOKEN', 'PLACEHOLDER_TOKEN')}",
    "Cookie": "ci_session=2imken2lq7l4f1jj7ns4id9rejq9asfm"
}

def calc_age(birth):
    today = datetime.date.today()
    return today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))

# ── Load look-up tables once ───────────────────────────────────────────────────
print("Step 4: Load look-ups once.")
DND = pd.read_csv(DND_FILE, dtype={'Phone': str}).rename(columns={'Phone':'Phone', 'Flag':'Flag'})
PIN = (pd.read_csv(PINCODE_FILTER_FILE)[['pincode']]
          .drop_duplicates()
          .rename(columns={'pincode':'Pincode'}))
PIN['Flag'] = 1
STATE = pd.read_csv(PINCODE_FILE)[['pincode', 'state_name', 'city']]
STATE.columns = ['E10', 'city', 'state']

# ── Batch loop ─────────────────────────────────────────────────────────────────
CHUNK = 50
batch = 0
print(f"Step 5: Start batching (chunksize={CHUNK}).")

for chunk in pd.read_csv(SRC_FILE, chunksize=CHUNK):
    batch += 1
    print(f"\n── Batch {batch} ──   RAM(before) {psutil.virtual_memory().percent}%")
    df = chunk.copy()

    # If TT in the specified range, merge STATE and align columns
    if TT in [2,3,4,5]:
        df = df.merge(STATE, on='E10', how='left')
        df['Master User ID'] = df['User Status'] = ''
        df['Current Application Status'] = df['Lps At'] = df['Submitted At'] = df['Last Assessed At'] = ''
        df['Partner ID'] = ''
        df = df[['Master User ID','User Status','E4','E5','E2','E6','E7','E8','E3',
                 'Current Application Status','E11','E27','E9','E22','E23','E10',
                 'city','state','Lps At','Submitted At','Last Assessed At']]

    # Rename final working columns
    df.columns = ["Master User ID","User Status","First Name","Last Name","Phone","Gender",
                  "Date Of Birth","Pan","Email ID","Current Application Status",
                  "Monthly Income","Employer Name","Employment Type","Address Line 1",
                  "Address Line 2","Pincode","City","State","Lps At","Submitted At",
                  "Last Assessed At"]

    # Basic cleaning
    df = df[df['Email ID'].notna() & df['Monthly Income'].notna()]
    df['Monthly Income'] = df['Monthly Income'].astype(int, errors='ignore')
    df['Phone'] = df['Phone'].astype(str)

    # Merge look-ups
    df = (df.merge(DND, on='Phone', how='left')
            .loc[lambda d: (d.Flag != 1) & (d.Flag != 0)]
            .drop(columns='Flag'))
    df = df[~df['Email ID'].str.contains('india', na=False)]

    df = (df.merge(PIN, on='Pincode', how='left')
            .loc[lambda d: d.Flag == 1]
            .drop(columns='Flag'))

    # Monthly income & email fix
    df['Monthly Income'] = df['Monthly Income'].apply(lambda x: min(int(x/12),199000) if x>=500000 else x)
    df['Email ID'] = (df['Email ID'].str.lower()
                                    .str.replace(r'[^\w\.@]+','',regex=True)
                                    .str.replace(r'gmil|gamil|gmai|gamail|gmial','gmail',regex=True))

    df = df[(df['Monthly Income']>=20000) & (df['Employment Type'].str.lower()=='salaried')]
    df['Gender'] = df['Gender'].replace({'f':'Female','m':'Male'})

    # Date parsing ➜ age filtering
    df['Date Of Birth'] = pd.to_datetime(df['Date Of Birth'], errors='coerce')
    df = df.dropna(subset=['Date Of Birth'])

    # Ensure no leftover 'Age' from source
    if 'Age' in df.columns:
        df = df.drop(columns=['Age'])

    df['AgeYears'] = df['Date Of Birth'].apply(calc_age)
    df = df[(df['AgeYears']>=21) & (df['AgeYears']<=58)]
    df['Date Of Birth'] = df['Date Of Birth'].dt.strftime('%d/%m/%Y')

    # Generate ref_id & placeholder API fields
    df['ref_id']     = np.random.randint(100000000, 999999999, size=df.shape[0])
    df['DDStatus']   = df['DDMessage'] = ''
    df['LCStatus']   = df['LCMessage'] = df['LCReason'] = ''

    # Row-wise API calls
    for i in range(df.shape[0]):
        mobile = str(df.at[i,'Phone'])[:10]
        ref_id = str(df.at[i,'ref_id'])

        try:
            r1 = requests.post(URL_CHECK,
                               json={"mobile":mobile,"partner_id":"AADIFINANCE","ref_id":ref_id},
                               headers=HEADERS, timeout=10)
            j1 = r1.json()
            df.at[i,'DDStatus']  = j1.get('status','')
            df.at[i,'DDMessage'] = j1.get('message','')
            print(f"  Row {i}: checkmobile → {j1.get('status','')}")
        except Exception as e:
            print(f"  Row {i}: checkmobile error {e}")
            continue

        if df.at[i,'DDStatus'] == 'S':
            payload = {
                "partner_id": "AADIFINANCE",
                "ref_id":     ref_id,
                "mobile":     mobile,
                "customer_name": f"{df.at[i,'First Name']} {df.at[i,'Last Name']}",
                "pancard":    df.at[i,'Pan'],
                "dob":        df.at[i,'Date Of Birth'],
                "pincode":    df.at[i,'Pincode'],
                "profession": "SAL",
                "net_mothlyincome": str(df.at[i,'Monthly Income'])
            }
            try:
                r2 = requests.post(URL_LOAN, json=payload, headers=HEADERS, timeout=10)
                j2 = r2.json()
                df.at[i,'LCStatus']  = j2.get('Status','')
                df.at[i,'LCMessage'] = j2.get('Message','')
                df.at[i,'LCReason']  = j2.get('reason','')
                print(f"  Row {i}: loanprocess → {j2.get('Status','')}")
            except Exception as e:
                print(f"  Row {i}: loanprocess error {e}")

    # Save batch
    out_file = os.path.join(SAVE_DIR, f"{l01}_{batch}.csv")
    df.to_csv(out_file, index=False)
    print(f"Saved Batch {batch} ➜ {out_file}")
    print(f"RAM(after) {psutil.virtual_memory().percent}%")

    # Memory cleanup
    del df
    gc.collect()

print("\nDone – all batches processed.")
