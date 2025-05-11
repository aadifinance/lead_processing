
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 17:34:28 2024

@author: Dotpe
"""



import pandas as pd
from datetime import date 
import datetime
from datetime import timedelta 
import requests
import json
import hmac
import hashlib
import base64
import json
import os
import re
import time
import numpy as np

import base64

BBuckets = ['Bucket1.csv','Bucket2.csv','Bucket3.csv','Bucket4.csv',"Bulk.csv"]
BBucketsSlab = [1,2,3,7,10]

hhh = 0
TT = 2

kk = 0
kk1= kk+1

l01 = ''

l00= 00000
l01 = l00+600000000

for hhh in range(kk,kk1):
    DD = str((date.today() - timedelta(days = hhh))).replace("-","")
    # FileName  = os.listdir("C:/Users/Dotpe/E/KreditBee/Data/"+DD+'/')

    if(TT == 0 ):
        FileName  = "C:/Users/Dotpe/E/KreditBee/Data/01. BData/"+DD+'.csv'
        savelocation= 'C:/Users/Dotpe/E/LendingPlate/ApprovedData/PS/'
    
    elif(TT == 1):
        FileName = 'C:/Users/Dotpe/E/KreditBee/Bulk/No BNPL/' + DD + '.csv'
        savelocation= 'C:/Users/Dotpe/E/LendingPlate/ApprovedData/NoBNPL/'
    
    elif(TT == 2):
         FileName = 'C:/Users/Dotpe/E/MP/Data/' + DD + '.csv'
         savelocation= 'C:/Users/Dotpe/E/LendingPlate/ApprovedData/MP/'
    elif(TT == 3):
         DD = '20250502'
         FileName = 'C:/Users/Dotpe/E/KreditBee/Data/0. Shub/shub/Data/' + DD + '.csv'
         savelocation= 'C:/Users/Dotpe/E/KreditBee/Data/0. Shub/shub/LendingPlate/'
         print("start")
    elif(TT == 4):
         NUM = 87
         DD = '%s. Batch %s_1'%(NUM,NUM)
         FileName = 'C:/Users/Dotpe/E/KreditBee/Data/0. Shub/John/Data/' + DD + '.csv'
         savelocation= 'C:/Users/Dotpe/E/KreditBee/Data/0. Shub/Jhon/LendingPlate/'
         print("start")
    elif(TT == 5):
         savelocation = 'C:/Users/Dotpe/E/KreditBee/Data/02. DigitalAdd/Approved Data/LendingPlate/'
         FileName = 'C:/Users/Dotpe/E/KreditBee/Data/02. DigitalAdd/Output/'+DD+'.csv'
         print("start")


    url = "https://lms.lendingplate.co.in/api/Api/affiliateApi/checkmobile"
    url_LP = "https://lms.lendingplate.co.in/api/Api/affiliateApi/loanprocess"
    
    # Headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer b88e51099b1511c32acb6d2a6c5546267d8611518a6f8b7e035a221d565b9b4f",
        "Cookie": "ci_session=2imken2lq7l4f1jj7ns4id9rejq9asfm"
    }
    
    kkk = 0
    ###############
        

    for kkk in range(0,1):
        ll = FileName

        if(TT == 2 or TT == 3 or TT == 4 or TT == 5 ):
            df = pd.read_csv(ll)
            STATE = pd.read_csv('C:/Users/Dotpe/E/CASHe/pincode.csv')
            STATE = STATE[['pincode','state_name','city']]
            STATE.columns = ['E10','city','state']
            df = df.merge(STATE, on='E10', how='left')
            del[STATE]
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


        else:
            if(re.compile("csv").search(ll) is None):
                ll = ll.replace('csv','xlsx')
                df = pd.read_excel(ll)
            else:        
                df = pd.read_csv(ll)
                
            try:            
                df['lps_at'] = ''
                df['submitted_at'] = ''
                df['current_application_status'] = ''
                df['last_assessed_at'] = ''
                df['user_status'] =''            
                df = df[["master_user_id"   ,"user_status"  ,"first_name"   
                      ,"last_name"  ,"phone"    ,"gender"   ,"date_of_birth"    
                      ,"pan"    ,"email_id",    "current_application_status",   
                      "monthly_income","employer_name"  ,"employment_type"  
                      ,"address_line_1" ,"address_line_2"   ,"pincode"  ,"city",
                      "state",  "lps_at","submitted_at" ,"last_assessed_at"]]
            except:
                
                try:
                    
                    STATE = pd.read_csv('C:/Users/Dotpe/E/CASHe/pincode.csv')
                    STATE = STATE[['pincode','state_name']]
                    STATE.columns = ['pincode','state']
                    df = df.merge(STATE, on='pincode', how='left')
                    del[STATE]
                    
                    df['lps_at'] = ''
                    df['submitted_at'] = ''
                    df['current_application_status'] = ''
                    df['last_assessed_at'] = ''
                    df['user_status'] =''            
                    df = df[["customer_master_id"   ,"user_status"  ,"first_name"   
                          ,"last_name"  ,"phone"    ,"gender"   ,"dob"  
                          ,"pan"    ,"email_id",    "current_application_status",   
                          "stated_income","employer_name"   ,"employer_type"    
                          ,"address_line_1" ,"address_line_2"   ,"pincode"  ,"city",
                          "state",  "source","ams_product_type" ,"bureau_pull"]]
                    
                    
                    df.loc[df['employer_type'] == 'SELF_EMP', 'employer_type'] = 'self_employed'  
                    df.loc[df['employer_type'] == 'SALARIED', 'employer_type'] = 'salaried' 
                    df['phone'] = df['phone'].apply(str).str[2:12]
                    
                except:                
                        df= df[["Master User ID"    ,"User Status"  ,"First Name"   
                                  ,"Last Name"  ,"Phone"    ,"Gender"   ,"Date Of Birth"    
                                  ,"Pan"    ,"Email ID",    "Current Application Status",   
                                  "Monthly Income","Employer Name"  ,"Employment Type"  
                                  ,"Address Line 1" ,"Address Line 2"   ,"Pincode"  ,"City",
                                  "State",  "Lps At","Submitted At" ,"Last Assessed At"]]
        
            
  
        ########Data Read Ends
        df.columns = ["Master User ID"  ,"User Status"  ,"First Name"   
                      ,"Last Name"  ,"Phone"    ,"Gender"   ,"Date Of Birth"    
                      ,"Pan"    ,"Email ID",    "Current Application Status",   
                      "Monthly Income","Employer Name"  ,"Employment Type"  
                      ,"Address Line 1" ,"Address Line 2"   ,"Pincode"  ,"City",
                      "State",  "Lps At","Submitted At" ,"Last Assessed At"]
        print(df.shape[0])
        if(TT==3 or TT == 4):
            df = df[l00:l01]
        
        df =  df[df['Email ID'].notna()]
        df =  df[df['Monthly Income'].notna()]
        df['Monthly Income'] =  df['Monthly Income'].apply(int)


        
        DND = pd.read_csv('C:/Users/Dotpe/E/KreditBee/DND.csv')
        DND.columns = ['Phone','Flag']
        DND['Phone']=DND['Phone'].astype(str)

        DND.columns = ['Phone','Flag']
        # DND['Phone']=DND['Phone'].astype(np.int64) 
        df['Phone']=df['Phone'].astype(str) 

        df = df.merge(DND, on='Phone', how='left')
        df = df.loc[(df.Flag != 1) ]
        df = df.loc[(df.Flag != 0) ]
        df = df[~df["Email ID"].str.contains('india')]
        df.drop('Flag',inplace=True,axis = 1) 

        Pincode = pd.read_csv(r'C:\Users\Dotpe\E\LendingPlate\Pincode.csv')    
        Pincode['Flag'] = 1
        Pincode = Pincode[['pincode',"Flag"]]
        Pincode.columns = ['Pincode','Flag']
        df = df.merge(Pincode, on='Pincode', how='left')
        df = df.loc[(df.Flag == 1) ]
        df.drop('Flag',inplace=True,axis = 1) 


        for i in range(0,df.shape[0]):
            if(int(df.iloc[i,10]) >= 500000):
                df.iloc[i,10] = min(int(int(df.iloc[i,10])/12),199000)


        
        char_to_replace = {'_': '',
                           ':': '',
                           ' ':'',
                           '~': '',
                           # '*':'',
                           '&':'',
                           ',':'',
                           "'":'',
                           '=':'',
                           '$':'',
                           # '?':'',
                           '@@':'@',
                           '/':'',
                           'Â¹':'',
                           'gmil.com':'gmail.com',
                           'gmale.com':'gmail.com',
                           # '\':'',
                           '|':'',
                            'c9m':'com','c0m':'com','-':'','#':'','.@':'@'
                            ,'.com.':'.com'
                            ,'.vcom':'.com','.co.m':'.com','!':'',
                           '|':'',re.compile(r'\.{3,}'):'.',
                            '@email.co': '@gmail.com','@email.com': '@gmail.com','@email.con': '@gmail.com','@email.om': '@gmail.com','@gail.co': '@gmail.com','@gail.co.in': '@gmail.com','@gail.com': '@gmail.com','@gamail.co': '@gmail.com','@gamail.com': '@gmail.com','@gamale.com': '@gmail.com','@gamil.cam': '@gmail.com','@gamil.co': '@gmail.com','@gamil.com': '@gmail.com','@gamil.con': '@gmail.com','@gamil.coom': '@gmail.com','@gamil.om': '@gmail.com','@gimal.cam': '@gmail.com','@gimal.co': '@gmail.com','@gimal.com': '@gmail.com','@gimal.comm': '@gmail.com','@gimal.con': '@gmail.com','@gimal.coom': '@gmail.com','@gimil.cam': '@gmail.com','@gimil.co': '@gmail.com','@gimil.com': '@gmail.com','@gmai.co': '@gmail.com','@gmai.com': '@gmail.com','@gmai.comm': '@gmail.com','@gmai.con': '@gmail.com','@gmail.cam': '@gmail.com','@gmail.co': '@gmail.com','@gmail.co.in': '@gmail.com','@gmail.comm': '@gmail.com','@gmail.con': '@gmail.com','@gmail.coom': '@gmail.com','@gmail.in.com': '@gmail.com','@gmail.om': '@gmail.com','@gmal.cam': '@gmail.com','@gmal.co': '@gmail.com','@gmal.com': '@gmail.com','@gmil.cam': '@gmail.com','@gmil.co': '@gmail.com','@gmIl.com': '@gmail.com','@gmil.coom': '@gmail.com','@hotmai.com': '@hotmail.com','@hotmail.co': '@hotmail.com','@hotmail.con': '@hotmail.com','@jimal.com': '@gmail.com','@jmail.com': '@gmail.com','@mail.Cam': '@gmail.com','@mail.co': '@gmail.com','@mail.com': '@gmail.com','@outlook.con': '@outlook.com','@rediffmai.com': '@rediffmail.com','@rediffmail.co': '@rediffmail.com','@rediffmail.comm': '@rediffmail.com','@rediffmail.con': '@rediffmail.com','@rediffmail.coom': '@rediffmail.com','@rediffmsil.com': '@rediffmail.com','@redifmail.com': '@rediffmail.com','@yaahoo.com': '@yahoo.com','@yahoo.co': '@yahoo.com','@yahoo.con': '@yahoo.com','@yahooo.co.in': '@yahoo.com','@yahooo.com': '@yahoo.com','@yahu.com': '@yahoo.com','@yhoo.co.in': '@yahoo.com','@yhoo.com': '@yahoo.com','@zmail.com': '@gmail.com'
                           ,'.comm':'com'
                           ,'.comail.com':'@gmail.com','.coml.com':'@gmail.com'
                           ,'.com.com':'.com'
                           ,'Email.com':'gmail.com','YAHOO.CO':'yahoo.com','GMAIL.CO':'gmail.com','yahoo.cam':'yahoo.com','gmai.Com':'gmail.com','gMIL.com':'gmail.com','rediffmail.comM':'rediffmail.com','yahoo.om':'yahoo.com','Gmal.com':'gmail.com','gmail.comM':'gmail.com','Gimal.com':'gmail.com','GAMIL.COM':'gmail.com','MAIL.COM':'gmail.com'
                           ,'gmailcom':'gmail.com','yahoocom':'yahoo.com','hotmailcom':'hotmail.com','.@':'@'
                           }
        for key, value in char_to_replace.items():
            df["Email ID"] = df["Email ID"].str.replace(key, value, regex = True)

        df["Email ID"] = df["Email ID"].str.lstrip('0123456789.- ')
        
    
        df['Date Of Birth'] =  df['Date Of Birth'].apply(str).str[:10]
       
        df = df.loc[(df['Monthly Income'] >= 20000) ]
        df = df.loc[(df['Employment Type'] == 'salaried') ]
        #Bureau 400        
        
        
        # Assuming df is already defined
        df.iloc[:, 12] = df.iloc[:, 12].replace({'salaried': 'SAL'}, regex=False)
        df.iloc[:, 12] = df.iloc[:, 12].replace({'self-employed': 'SENP'}, regex=False)
        
        df.iloc[:, 5] = df.iloc[:, 5].replace({'f': 'Female', 'm': 'Male'}, regex=False)
        
        # Optimized date formatting (assuming column 6 contains dates in YYYY-MM-DD format)
        # df.iloc[:, 6] = pd.to_datetime(df.iloc[:, 6]).dt.strftime('%d/%m/%Y')

                
        df['Age'] = ''

        # Function to calculate age
        def calculate_age(birth_date):
            today = datetime.date.today()
            return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        
        df.iloc[:, 6] = pd.to_datetime(df.iloc[:, 6], errors='coerce')
        
        df.iloc[:, 21] = df.iloc[:, 6].apply(lambda x: calculate_age(x) if pd.notnull(x) else 25)
        
        df = df.loc[(df['Age'] >= 21) ]
        df = df.loc[(df['Age'] <= 58) ]
        df.drop('Age',inplace=True,axis = 1)

        df.iloc[:, 6] = pd.to_datetime(df.iloc[:, 6], format='%Y-%m-%d', errors='coerce').dt.strftime('%d/%m/%Y')
        
        df['ref_id'] = np.random.randint(100000000, 999999999, size=df.shape[0])

                
        x = df
        
        x['DDStatus'] = ''
        x['DDMessage'] = ''
        x['LCStatus'] = ''
        x['LCMessage'] = ''
        x['LCReason'] = ''

        # x = pd.read_csv(savelocation+DD+'/'+BBuckets[kkk])
        # # # # # # # # x.drop('Unnamed: 0',inplace=True,axis = 1)

        l1 = x.shape[0]
        l0 = i = 0
        
        x = x[x['Monthly Income'].notna()]
        x = x[x['Date Of Birth'].notna()]
        x = x[x['Phone'].notna()]
                    
        for i in range(i+1, l1):
            # time.sleep(0.5)
            print(i)
            # url = baseurl
            # Request payload
            data = {
                "mobile": str(x.iloc[i,4])[:10],
                "partner_id": "AADIFINANCE",
                "ref_id": str(x.iloc[i,21])
            }
            
            # Sending POST request
            try:
                response = requests.post(url, json=data, headers=headers)
                responseMobile = json.loads(response.text)
                print(responseMobile)
            except:
                time.sleep(5)
                
            k = 21
            try:
                x.iloc[i,k+1] = responseMobile["status"]
            except:
                x.iloc[i,k+1] = ''
            try:
                x.iloc[i,k+2] = responseMobile["message"]
            except:
                x.iloc[i,k+2] = ''
            
                
            if(responseMobile["status"] == "S"):
                loan_payload = {
                    "partner_id": "AADIFINANCE",
                    "ref_id": str(x.iloc[i,21]),
                    "mobile": str(x.iloc[i,4])[:10],
                    "customer_name": str(x.iloc[i,2]) + ' '+str(x.iloc[i,3]),
                    "pancard": str(x.iloc[i,7]),
                    "dob": str(x.iloc[i,6]),
                    "pincode": str(x.iloc[i,15]),
                    "profession": str(x.iloc[i,12]),
                    "net_mothlyincome": str(x.iloc[i,10])
                }
                
                # Sending POST request
                try:
                    response = requests.post(url_LP, json=loan_payload, headers=headers)
        
                    responseMobile1 = json.loads(response.text)
                    print(responseMobile1 )
                except:
                    time.sleep(5)
                k = 21
                try:
                    x.iloc[i,k+3] = responseMobile1["Status"]
                except:
                    x.iloc[i,k+3] = ''
                try:
                    x.iloc[i,k+4] = responseMobile1["Message"]
                except:
                    x.iloc[i,k+4] = ''
                try:
                    x.iloc[i,k+5] = responseMobile1["reason"]
                except:
                    x.iloc[i,k+5] =''
            
                
            os.makedirs(savelocation+DD+'/',exist_ok  = True)
            # x.to_csv(savelocation+DD+'/'+BBuckets[kkk], index = False)
            if(i%500==0):                    
                x.to_csv(savelocation+DD+'/'+str(l01) +'.csv', index = False)

DD = str((date.today() - timedelta(days = 0))).replace("-","")
os.makedirs(savelocation+DD+'/',exist_ok  = True)
x.to_csv(savelocation+DD+'/'+str(l01) +'.csv', index = False)
    
##########
