# -*- coding: utf-8 -*-
"""
Lead-processing pipeline
 • Arrow-backed strings (including pincode)
 • Small CSV chunks (50 rows)
 • One-time look-ups
 • Memory logging & GC
31-May-2025
"""

import os, gc, psutil, requests, numpy as np, pandas as pd
import datetime as _dt
from datetime import date, timedelta

# ───────────────────── 0. PATHS & CONSTANTS ─────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PINCODE_FILE        = os.path.join(BASE_DIR, "data", "pincode.csv")
DND_FILE            = os.path.join(BASE_DIR, "data", "DND.csv")
PINCODE_FILTER_FILE = os.path.join(BASE_DIR, "LendingPlate", "Pincode.csv")

TT          = 4
BATCH_LABEL = 90         # builds DD string
CHUNK       = 50         # rows per batch
FILE_SEED   = 600000000  # prefix for output filenames

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

# ───────────────────── 1. API CONFIG ────────────────────────────
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

# ───────────────────── 2. LOOK-UPS (read ONCE) ──────────────────
# Force pincode columns to Arrow string everywhere ➜ zfill(6)
READ_STR = {"pincode": "string[pyarrow]"}

DND = (pd.read_csv(DND_FILE,               dtype_backend="pyarrow")
         .rename(columns={"Phone": "Phone", "Flag": "Flag"}))

PIN = (pd.read_csv(PINCODE_FILTER_FILE,    dtype=READ_STR, dtype_backend="pyarrow")[["pincode"]]
         .drop_duplicates()
         .rename(columns={"pincode": "Pincode"}))
PIN["Pincode"] = PIN["Pincode"].str.zfill(6)
PIN["Flag"] = 1

STATE = (pd.read_csv(PINCODE_FILE,         dtype=READ_STR, dtype_backend="pyarrow")[["pincode","state_name","city"]]
           .rename(columns={"pincode": "E10", "state_name": "state", "city": "city"}))
STATE["E10"] = STATE["E10"].str.zfill(6)

# ───────────────────── 3. CSV READ PARAMETERS ───────────────────
dtype_map = {
    "E10": "string[pyarrow]",  # pincode as Arrow string
    "E11": "Int64",            # salary as nullable int
}
PARSE_DATES = ["E7"]           # DOB column

proc = psutil.Process(os.getpid())
print(f"My RAM: {proc.memory_info().rss/1024**2:.1f} MB")

# ───────────────────── 4. BATCH LOOP ────────────────────────────
batch = 0
for chunk in pd.read_csv(
        SRC_FILE,
        chunksize       = CHUNK,
        dtype           = dtype_map,
        parse_dates     = PARSE_DATES,
        dtype_backend   = "pyarrow",      # Arrow strings everywhere else
        keep_default_na = False,
        dayfirst        = False):
    batch += 1
    print(f"\n–– Batch {batch} ––  RAM before: {psutil.virtual_memory().percent}%")

    df = chunk.copy()

    # Ensure pincode column is Arrow string + zero-padded
    df["E10"] = df["E10"].astype("string[pyarrow]").str.zfill(6)

    # ── Optional alignment for TT 2/3/4/5 ──
    if TT in [2, 3, 4, 5]:
        df = df.merge(STATE, on="E10", how="left")
        df["Master User ID"] = df["User Status"] = ""
        df["Current Application Status"] = df["Lps At"] = df["Submitted At"] = df["Last Assessed At"] = ""
        df["Partner ID"] = ""
        df = df[["Master User ID","User Status","E4","E5","E2","E6","E7","E8","E3",
                 "Current Application Status","E11","E27","E9","E22","E23","E10",
                 "city","state","Lps At","Submitted At","Last Assessed At"]]

    # Rename columns
    df.columns = ["Master User ID","User Status","First Name","Last Name","Phone","Gender",
                  "Date Of Birth","Pan","Email ID","Current Application Status",
                  "Monthly Income","Employer Name","Employment Type","Address Line 1",
                  "Address Line 2","Pincode","City","State","Lps At","Submitted At",
                  "Last Assessed At"]

    # ── Basic cleaning ──
    df = df[df["Email ID"].ne("") & df["Monthly Income"].ne("")]
    df["Phone"]          = df["Phone"].astype("string[pyarrow]")
    df["Monthly Income"] = df["Monthly Income"].astype("Int64", errors="ignore")

    # DND & bad email filters
    df = (df.merge(DND, on="Phone", how="left")
            .loc[lambda d: (d.Flag != 1) & (d.Flag != 0)]
            .drop(columns="Flag"))
    df = df[~df["Email ID"].str.contains("india", na=False)]

    # Pincode filter
    df = (df.merge(PIN, on="Pincode", how="left")
            .loc[lambda d: d.Flag == 1]
            .drop(columns="Flag"))

    # Income & email fixes
    df["Monthly Income"] = df["Monthly Income"].apply(
        lambda x: min(int(x/12),199000) if pd.notna(x) and x>=500000 else x
    )
    df["Email ID"] = (df["Email ID"]
                        .str.lower()
                        .str.replace(r"[^\w\.@]+","",regex=True)
                        .str.replace(r"gmil|gamil|gmai|gamail|gmial","gmail",regex=True))

    # Employment & income gates
    df = df[(df["Monthly Income"]>=20000) & (df["Employment Type"].str.lower()=="salaried")]
    df["Gender"] = df["Gender"].replace({"f":"Female","m":"Male"})

    # Age gate
    df["Date Of Birth"] = pd.to_datetime(df["Date Of Birth"], errors="coerce")
    df = df.dropna(subset=["Date Of Birth"])
    if "Age" in df.columns:           # drop stray Age column
        df = df.drop(columns=["Age"])
    df["AgeYears"] = df["Date Of Birth"].dt.date.apply(lambda x: calc_age(x))
    df = df[(df["AgeYears"]>=21) & (df["AgeYears"]<=58)]
    df["Date Of Birth"] = df["Date Of Birth"].dt.strftime("%d/%m/%Y")

    # IDs & placeholders
    df["ref_id"] = np.random.randint(1e8,1e9-1,size=df.shape[0],dtype=np.int64)
    df[["DDStatus","DDMessage","LCStatus","LCMessage","LCReason"]] = ""

    # ── Row-wise API calls ──
    phone_ix  = df.columns.get_loc("Phone")
    refid_ix  = df.columns.get_loc("ref_id")
    status_ix = df.columns.get_loc("DDStatus")
    msg_ix    = df.columns.get_loc("DDMessage")
    lcstat_ix = df.columns.get_loc("LCStatus")
    lcmsg_ix  = df.columns.get_loc("LCMessage")
    lcreas_ix = df.columns.get_loc("LCReason")

    for i in range(df.shape[0]):
        mobile = str(df.iat[i, phone_ix])[:10]
        ref_id = str(df.iat[i, refid_ix])

        try:
            j1 = requests.post(URL_CHECK,
                               json={"mobile":mobile,"partner_id":"AADIFINANCE","ref_id":ref_id},
                               headers=HEADERS, timeout=10).json()
            df.iat[i, status_ix] = j1.get("status","")
            df.iat[i, msg_ix]    = j1.get("message","")
            print(f" Row {i}: checkmobile → {j1.get('status','')}")
        except Exception as e:
            print(f" Row {i}: checkmobile error {e}")
            continue

        if j1.get("status") == "S":
            payload = {
                "partner_id"      : "AADIFINANCE",
                "ref_id"          : ref_id,
                "mobile"          : mobile,
                "customer_name"   : f"{df.iat[i,2]} {df.iat[i,3]}",
                "pancard"         : df.iat[i, df.columns.get_loc('Pan')],
                "dob"             : df.iat[i, df.columns.get_loc('Date Of Birth')],
                "pincode"         : df.iat[i, df.columns.get_loc('Pincode')],
                "profession"      : "SAL",
                "net_mothlyincome": str(df.iat[i, df.columns.get_loc('Monthly Income')]),
            }
            try:
                j2 = requests.post(URL_LOAN, json=payload, headers=HEADERS, timeout=10).json()
                df.iat[i, lcstat_ix] = j2.get("Status","")
                df.iat[i, lcmsg_ix]  = j2.get("Message","")
                df.iat[i, lcreas_ix] = j2.get("reason","")
                print(f" Row {i}: loanprocess → {j2.get('Status','')}")
            except Exception as e:
                print(f" Row {i}: loanprocess error {e}")

    # ── Save & free ──
    out_path = os.path.join(SAVE_DIR, f"{FILE_SEED}_{batch}.csv")
    df.to_csv(out_path, index=False)
    print(f"Saved ➜ {out_path} | RAM after: {psutil.virtual_memory().percent}%")

    del df
    gc.collect()

print("\n✔ All batches processed successfully.")
