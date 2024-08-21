import polars as pl

Input = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\Turnover_CHECKS\SW\SW_AC_ALLCAP.csv", try_parse_dates=True)
Turnover = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\Turnover_CHECKS\Output\Turnover_SID.parquet", try_parse_dates=True)
print(Turnover)