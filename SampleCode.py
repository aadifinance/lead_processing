# -*- coding: utf-8 -*-
"""
Lead-processing pipeline with Arrow strings everywhere.
Refactored: 31-May-2025
"""

import os, gc, psutil, requests, numpy as np, pandas as pd
import datetime as _dt
from datetime import date, timedelta

# ─────────────────────────────── 0. PATHS & CONSTANTS ─────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
print("BASE_DIR:", BASE_DIR)

PINCODE_FILE        = os.path.join(BASE_DIR, "data", "pincode.csv")
DND_FILE            = os.path.join(BASE_DIR, "data", "DND.csv")
PINCODE_FILTER_FILE = os.path.join(BASE_DIR, "LendingPlate", "Pincode.csv")

TT               = 4               # data-source selector
BATCH_LABEL      = 90              # label for building DD string
CHUNK            = 50              # rows per batch
FILE_SEED        = 600000000       # prefix for output filenames

DD = f"{BATCH_LABEL}. Batch {BATCH_LABEL}_1" if TT == 4 else str(date.today()).replace("-", "")

data_sources = {
    0: ("data/KreditBee/Data/01. BData",             "LendingPlate/ApprovedData/PS"),
    1: ("data/KreditBee/Bulk/No BNPL",               "LendingPlate/ApprovedData/NoBNPL"),
    2: ("MP/Data",                                   "LendingPlate/ApprovedData/MP"),
    3: ("data/KreditBee/Data/0. Shub/shub/Data",     "KreditBee/Data/0. Shub/shub/LendingPlate"),
    4: ("data/KreditBee/Data/0. Shub/John/Data",     "KreditBee/Data/0. Shub/Jhon/LendingPlate"),
    5: ("data/KreditBee/Data/02. DigitalAdd/Output", "KreditBee/Data/02. DigitalAdd/Approved Data/LendingPlate"),
}

SRC_FILE = os.path.join(BASE_DIR, data_sources[TT][0], f"{DD}.csv")
SAVE_DIR = os.path.join(BASE_DIR, data_sources[TT][1], DD)
os.makedirs(SAVE_DIR, exist_ok=True)

print("SRC_FILE :", SRC_FILE)
print("SAVE_DIR :", SAVE_DIR)

# ─────────────────────────────── 1. API CONFIG ────────────────────────────────
URL_CHECK = "https://lms.lendingplate.co.in/api/Api/affiliateApi/checkmobile"
URL_LOAN  = "https://lms.lendingplate.co.in/api/Api/affiliateApi/loanprocess"
HEADERS   = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {os.getenv('API_TOKEN', 'PLACEHOLDER_TOKEN')}",
    "Cookie": "ci_session=2imken2lq7l4f1jj7ns4id9rejq9asfm",
}

def calc_age(dob: _dt.date) -> int:
    today = _dt.date.today()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

# ─────────────────────────────── 2. LOOKUPS ONCE ──────────────────────────────
DND = (pd.read_csv(DND_FILE, dtype_backend="pyarrow")  # Arrow strings automatically
         .rename(columns={"Phone": "Phone", "Flag": "Flag"}))

PIN = (pd.read_csv(PINCODE_FILTER_FILE, dtype_backend="pyarrow")[["pincode"]]
         .drop_duplicates()
         .rename(columns={"pincode": "Pincode"}))
PIN["Flag"] = 1

STATE = (pd.read_csv(PINCODE_FILE, dtype_backend="pyarrow")[["pincode", "state_name", "city"]]
           .rename(columns={"pincode": "E10", "state_name": "state", "city": "city"}))

# ─────────────────────────────── 3. READ-CSV PARAMS ───────────────────────────
dtype_map = {
    "E10": "string[pyarrow]",  # Pincode kept as Arrow string (preserves leading zeros)
    "E11": "Int64",            # Salary as nullable integer for numeric logic
}
PARSE_DATES = ["E7"]           # DOB column

# ─────────────────────────────── 4. BATCH LOOP ────────────────────────────────
batch = 0

proc = psutil.Process(os.getpid())
print(f"My RAM: {proc.memory_info().rss/1024**2:.1f} MB")

