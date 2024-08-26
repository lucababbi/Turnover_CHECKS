import requests
import sys
import numpy as np
import pandas as pd
import datetime as dt
from datetime import datetime
from pandas.tseries.offsets import BDay, MonthEnd
from io import StringIO
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.auth import HTTPBasicAuth
import glob
from openpyxl import load_workbook

dates_df = pd.read_csv(r"C:\Users\et246\Desktop\Turnover_CHECKS\SID_Download\Dates_Download.csv")
dates_df["Review"] = pd.to_datetime(dates_df["Review"], format="%Y-%m-%d").dt.date
dates_df["Cutoff"] = pd.to_datetime(dates_df["Cutoff"], format="%Y-%m-%d").dt.date

review_dates = dates_df["Review"].unique().tolist()
cutoff_dates = dates_df["Cutoff"].unique().tolist()


# Code for fetching TOR Data as of Cutoff:

final_tor = pd.DataFrame()

for x in range(len(review_dates)): 

    url = 'https://codfix2.bat.ci.dom/stoxxcalcservice/api/CalcValue/GetCalcValues?vd={}&mapDt={}&tokenListCSV=TURNOVER_3M'.format(
    cutoff_dates[x].strftime('%Y%m%d'), pd.to_datetime(review_dates[x], format = '%Y/%m/%d').strftime('%Y%m%d'))

    data_tor = requests.get(url, verify = False)

    a = data_tor.text.splitlines()

    for i in range(len(a)):
        a[i] = list(a[i].split(','))

    df2 = pd.DataFrame(a, columns=a[0])
    df2 = df2.drop(df2.index[0])
    
    df2['mapDt'] = pd.to_datetime(df2['mapDt'], format='%Y%m%d').dt.date
    df2.rename(columns = {'mapDt':'Date', 'stoxxId':'Internal_Number', 'value':'Turnover_Ratio'}, inplace = True)

    final_tor = pd.concat([final_tor, df2])

    print('Done for ' + str(review_dates[x]))

final_tor.to_csv(r"C:\Users\et246\Desktop\Turnover_CHECKS\Output\Turnover_SID.csv", index = False)
print('Done.')






















