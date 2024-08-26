from sqlite3 import Date
import sys
sys.path.append(r"C:\Users\et246\Desktop\Turnover_CHECKS\STOXX")
import pandas as pd
from datetime import datetime
from pandasql import sqldf
from stoxx.qad.Turnover_Code import get_turnover_ratio

Output_Turnover = pd.DataFrame()

Output = pd.read_csv(r"C:\Users\et246\Desktop\Turnover_CHECKS\QAD_Download\SW_AC_ALLCAP_JUNDEC.csv", index_col=0, parse_dates=["composition_date"])
InfoCode = pd.read_csv(r"C:\Users\et246\Desktop\Turnover_CHECKS\QAD_Download\InfoCode.csv", parse_dates=["vt", "vf"], index_col=0)

# Deal with 99991230 dates with a date in remote future
InfoCode["vt"] = InfoCode["vt"].replace("99991230", "21001230")
# Convert columns into DateTime
InfoCode["vt"] = pd.to_datetime(InfoCode["vt"], format = "%Y%m%d")

Output = sqldf("""
                SELECT * FROM Output AS Input
                LEFT JOIN InfoCode AS Info
                ON Info.StoxxId = Input.stoxxid
                WHERE Input.composition_date >= Info.vf
                AND Input.composition_date <= Info.vt                         
            """
                    ).drop(columns={"InfoCodeSource", "SecCode", "SecCodeRegion", "SecCodeSource", "vf", "vt", "SecId", "Isin", "Ric", "sedol"}).dropna(subset=["InfoCode", "Sedol6"])

Output.composition_date = pd.to_datetime(Output.composition_date)
Output.composition_date = Output.composition_date.dt.strftime("%Y-%m-%d")
Output = Output.sort_values(by=("composition_date"))

# Dates Frame
Dates_Frame = pd.read_csv(r"C:\Users\et246\Desktop\Turnover_CHECKS\QAD_Download\Dates_Download.csv", sep=";")
# Dates_Frame.Review = pd.to_datetime(Dates_Frame.Review)
# Dates_Frame.Cutoff = pd.to_datetime(Dates_Frame.Cutoff)

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

    temp_Output = Output.query("composition_date == @date").dropna(subset="Sedol6")
    AA = get_turnover_ratio(temp_Output["Sedol6"].tolist(), temp_Output["InfoCode"].tolist(), startdate, enddate, sedoldate = None)
    AA["Cutoff"] = date
    Output_Turnover = pd.concat([Output_Turnover, AA])

    print(str(date) + " done!")

Output_Turnover = Output_Turnover.rename(columns={"Turnover_Ratio": "Turnover_Ratio_3M"})

Output_Turnover = Output_Turnover.merge(Output[["StoxxId", "InfoCode", "composition_date"]], 
                                        left_on=["Cutoff", "InfoCode"], right_on=["composition_date", "InfoCode"], 
                                        how="left").drop(columns={"composition_date"})

Output_Turnover.to_csv(r"C:\Users\et246\Desktop\Turnover_CHECKS\QAD_Download\Turnover_Cutoff_1Q_SWALL.csv")