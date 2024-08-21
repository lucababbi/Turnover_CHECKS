from tkinter.tix import Tree
import polars as pl

Input = pl.read_csv(r"C:\Users\et246\Desktop\Turnover_CHECKS\SW\SW_AC_ALLCAP_Historical.csv", try_parse_dates=True, infer_schema=False)
Input = Input.select(pl.col(["Date", "Internal_Number", "SEDOL"]))
Input.write_parquet(r"C:\Users\et246\Desktop\Turnover_CHECKS\SW\SW_AC_ALLCAP_Historical.parquet")