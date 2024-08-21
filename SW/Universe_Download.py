import datetime
from datetime import date, datetime, timedelta
import calendar
import polars as pl
from qpds import universe
from qpds import repo_connector

Final_Output = pl.DataFrame()

# Index Specifications
repo = repo_connector.connect("PROD")
symbol = "STXWAGV"
ccy = 'USD'
cal = 'STOXXCAL'

def first_monday_after_third_friday(year, month):
    """Find the first Monday after the third Friday of the given month and year."""
    # Initialize the count of Fridays found
    fridays_found = 0
    third_friday = None
    
    # Iterate through the days of the month
    for day in range(1, calendar.monthrange(year, month)[1] + 1):
        current_date = date(year, month, day)
        # Check if the current day is a Friday
        if current_date.weekday() == 4:  # Friday
            fridays_found += 1
            if fridays_found == 3:
                third_friday = current_date
                break
    
    if third_friday is None:
        raise ValueError("Could not find the third Friday in the given month.")

    # Find the next Monday after the third Friday
    first_monday = third_friday + timedelta(days=(7 - third_friday.weekday())) if third_friday.weekday() != 0 else third_friday
    return first_monday

def last_business_day_of_previous_month(year, month):
    """Find the last business day of the previous month."""
    previous_month = month - 1 if month > 1 else 12
    previous_year = year if month > 1 else year - 1
    last_day = calendar.monthrange(previous_year, previous_month)[1]
    last_day_date = date(previous_year, previous_month, last_day)
    
    # If the last day is a weekend, adjust to the previous Friday
    if last_day_date.weekday() >= 5:
        last_day_date -= timedelta(days=(last_day_date.weekday() - 4))
    
    return last_day_date

def create_review_cutoff_date_range(start_year, end_year):
    """Create a DataFrame with Review and Cutoff dates for March and September."""
    review_dates = []
    cutoff_dates = []
    
    for year in range(start_year, end_year + 1):
        for month in [3, 9]:  # March and September
            review_date = first_monday_after_third_friday(year, month)
            cutoff_date = last_business_day_of_previous_month(year, month)
            
            review_dates.append(review_date)
            cutoff_dates.append(cutoff_date)
    
    df = pl.DataFrame({
        "Review": review_dates,
        "Cutoff": cutoff_dates
    })
    
    return df

# Create Dates Range
Securities_Date = create_review_cutoff_date_range(1997, 2024)

for Review in Securities_Date["Review"]:
    Cutoff = Securities_Date.filter(pl.col("Review") == Review).select("Cutoff").to_series()[0]

    result = universe.get(repo,
                          symbol,
                          Review,
                          Cutoff,
                          cal, ccy)
    
    result = pl.DataFrame(result)
        
    if len(result) > 0:
        result = result.select(pl.col(["stoxxid", "sedol", "composition_date"]))

        Final_Output = pl.concat([Final_Output, result])

print(Final_Output)
Final_Output.write_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\Turnover_CHECKS\SW\SW_AC_ALLCAP.csv")