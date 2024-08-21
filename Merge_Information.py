import polars as pl

Input = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\Turnover_CHECKS\SW\SW_AC_ALLCAP_Historical.parquet")
Input = Input.with_columns(
                            [
                                pl.col("Date").cast(pl.Date),
                                pl.col("Internal_Number").cast(pl.Utf8),
                                pl.col("SEDOL").cast(pl.Utf8)
                            ]
                          )

Turnover = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\Turnover_CHECKS\Output\Turnover_SID.parquet")
Turnover = Turnover.select(["Date", "Internal_Number", "field", "Turnover_Ratio"]).with_columns(pl.col("Date").cast(pl.Date))

# Keep only relevant fields
Turnover = Turnover.filter(pl.col("field").is_in(["TurnoverRatioFO", 
                                               "TurnoverRatio", 
                                               "TurnoverRatioFO_India1", 
                                               "TurnoverRatio_India1"])
                          )

Turnover = Turnover.pivot(
                            values="Turnover_Ratio",
                            index=["Date", "Internal_Number"],
                            on="field"
)

Input = Input.join(Turnover, on=["Internal_Number", "Date"], how="left")

# Checks on Turnover
Input = Input.with_columns([
                            (pl.col("TurnoverRatioFO") >= pl.col("TurnoverRatio")).alias("CHECK_1"),
                            (pl.col("TurnoverRatioFO_India1") >= pl.col("TurnoverRatio_India1")).alias("CHECK_2"),
                            (pl.col("TurnoverRatioFO_India1") >= pl.col("TurnoverRatioFO")).alias("CHECK_3")
                            ])

# Store Results
Input.write_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\Turnover_CHECKS\CHECKS\Input_CHECKS.csv")
# Check 1
Input.select(
                pl.col("Date", "Internal_Number", "SEDOL", "TurnoverRatioFO", "TurnoverRatio", "CHECK_1")).filter(
                    pl.col("CHECK_1").eq(False),
                    pl.col("TurnoverRatioFO") > 0,
                    pl.col("TurnoverRatio") > 0
                ).write_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\Turnover_CHECKS\CHECKS\Input_CHECK_1.csv")

# Check 2
Input.select(
                pl.col("Date", "Internal_Number", "SEDOL", "TurnoverRatioFO_India1", "TurnoverRatio_India1", "CHECK_2")).filter(
                    pl.col("CHECK_2").eq(False),
                    pl.col("TurnoverRatioFO_India1") > 0,
                    pl.col("TurnoverRatio_India1") > 0
                ).write_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\Turnover_CHECKS\CHECKS\Input_CHECK_2.csv")

# Check 3
Input.select(
                pl.col("Date", "Internal_Number", "SEDOL", "TurnoverRatioFO_India1", "TurnoverRatioFO", "CHECK_3")).filter(
                    pl.col("CHECK_3").eq(False),
                    pl.col("TurnoverRatioFO_India1") > 0,
                    pl.col("TurnoverRatioFO") > 0
                ).write_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\Turnover_CHECKS\CHECKS\Input_CHECK_3.csv")