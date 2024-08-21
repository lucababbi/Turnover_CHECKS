import pandas as pd
import os

Folder = r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\Turnover_CHECKS\CHECKS"
Output = r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\Turnover_CHECKS\CHECKS\Merged_Checks.xlsx"

CSV = [f for f in os.listdir(Folder) if f.endswith(".csv")]

with pd.ExcelWriter(Output, engine='openpyxl') as writer:
    # Loop through each CSV file and write it to a separate sheet
    for File in CSV:
        Path = os.path.join(Folder, File)
        Name = os.path.splitext(File)[0]  # Use file name as sheet name
        Frame = pd.read_csv(Path)
        Frame.to_excel(writer, sheet_name=Name, index=False)
