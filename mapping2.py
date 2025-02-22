import pandas as pd

# Load the Excel files
file1 = "data/xls/SampleTest.xlsx"  # First Excel file

df1 = pd.read_excel(file1, sheet_name='Sheet1')
df2 = pd.read_excel(file1, sheet_name='Sheet2')

# Merge df1 and df2 on T_ID and S_ID
merged_df = df1.merge(df2, on=['T_ID', 'S_ID'], how='left', suffixes=('', '_y'))

# Update Value1, Value2, and Value3 where Value1 is missing in df1
df1.loc[df1['Value1'].isna(), ['Value1', 'Value2', 'Value3']] = merged_df.loc[df1['Value1'].isna(), ['Value1_y', 'Value2_y', 'Value3_y']].values

# Drop unnecessary columns if they exist
df1.drop(columns=['Value1_y', 'Value2_y', 'Value3_y'], errors='ignore', inplace=True)

# Save the updated file
df1.to_excel("updated_file.xlsx", index=False)

print("Updated file saved as 'updated_file.xlsx'")
