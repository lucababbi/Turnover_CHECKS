from sqlite3 import Date
import sys
# sys.path.append(r"C:\Users\et246\Desktop\MSCI-EM-SC-New\STOXX")
import pandas as pd
from datetime import datetime
from pandasql import sqldf
# from stoxx.qad.Turnover_Code import get_turnover_ratio

Output_Turnover = pd.DataFrame()

Output = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\ESG\ESG_Project\Universe\SW_DEV_SC.csv", index_col=0, parse_dates=["Date"])
InfoCode = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\ESG\ESG_Project\Universe\InfoCode.csv", parse_dates=["vt", "vf"], index_col=0)

# Deal with 99991230 dates with a date in remote future
InfoCode["vt"] = InfoCode["vt"].replace("99991230", "21001230")
# Convert columns into DateTime
InfoCode["vt"] = pd.to_datetime(InfoCode["vt"], format = "%Y%m%d")

Output = sqldf("""
                SELECT * FROM Output AS Input
                LEFT JOIN InfoCode AS Info
                ON Info.StoxxId = Input.Internal_Number
                WHERE Input.Date >= Info.vf
                AND Input.Date <= Info.vt                         
            """
                    ).drop(columns={"InfoCodeSource", "SecCode", "SecCodeRegion", "SecCodeSource", "vf", "vt", "SecId","Sedol6", "Isin", "Ric"}).dropna(subset=["SEDOL", "InfoCode"])

Output.Date = pd.to_datetime(Output.Date)

# Dates Frame
Dates_Frame = pd.read_csv(r"C:\Users\et246\Desktop\MSCI-EM-SC-New\ESG_Project\Review_Date.csv", index_col=0, parse_dates=["Review", "Cutoff"], sep=";")

for idx, Cutoff in enumerate(Dates_Frame["Cutoff"]):
    if Cutoff.weekday() == 5:
        Update_Cutoff = Cutoff - pd.DateOffset(days = 1)
        Dates_Frame.loc[idx, "Cutoff"] = Update_Cutoff
    elif Cutoff.weekday() == 6:
        Update_Cutoff = Cutoff - pd.DateOffset(days = 2)
        Dates_Frame.loc[idx, "Cutoff"] = Update_Cutoff

def last_business_day(date):
    # Convert date to pandas Timestamp
    date = pd.Timestamp(date)
    
    # Move the date to the last business day of the month
    last_day_of_month = pd.Timestamp(date.year, date.month - 1, 1) + pd.offsets.BMonthEnd()
    
    # Move backwards until we find a business day
    while True:
        if last_day_of_month.dayofweek < 5:  # Monday is 0 and Sunday is 6
            return last_day_of_month
        else:
            last_day_of_month -= pd.Timedelta(days=1)

def first_business_day(date):
    # Convert date to pandas Timestamp
    date = pd.Timestamp(date)
    
    # Move the date to the last business day of the month
    last_day_of_month = pd.Timestamp(date.year, date.month, 1)
    
    # Move backwards until we find a business day
    while True:
        if last_day_of_month.dayofweek < 5:  # Monday is 0 and Sunday is 6
            return last_day_of_month
        else:
            last_day_of_month += pd.Timedelta(days=1)

for date in Dates_Frame["Cutoff"]:
    review = pd.to_datetime(Dates_Frame.loc[Dates_Frame["Cutoff"] == date, "Review"].values[0])
    enddate = pd.to_datetime(date)
    startdate = enddate - pd.DateOffset(months = 3)

    # Check that startdate is not a Weekend
    if startdate.weekday() == 5:
        startdate = startdate - pd.DateOffset(days = 1)
    elif startdate.weekday() == 6:
        startdate = startdate - pd.DateOffset(days = 2)

    temp_Output = Output.query("Date == @review")
    AA = get_turnover_ratio(temp_Output["SEDOL"].tolist(), temp_Output["InfoCode"].tolist(), startdate, enddate, sedoldate = None)
    AA["Date"] = review
    Output_Turnover = pd.concat([Output_Turnover, AA])

Output_Turnover.to_csv(r"C:\Users\et246\Desktop\MSCI-EM-SC-New\ESG_Project\ToR\Turnover_Cutoff_1Q_SWALL.csv")