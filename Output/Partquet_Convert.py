import polars as pl

Input = pl.read_csv(r"C:\Users\et246\Desktop\Turnover_CHECKS\Output\Turnover_SID.csv")
Input.write_parquet(r"C:\Users\et246\Desktop\Turnover_CHECKS\Output\Turnover_SID.parquet")