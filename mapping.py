import pandas as pd

# Define file paths
sor_file = "sor.xlsx"  # Main SOR file
sor_other_file = "sor_other.xlsx"  # Another source for missing values
mapping_file = "mapping.xlsx"  # Mapping file
output_file = "updated_mapping.xlsx"  # Final output file

# Define columns
sor_key_column1 = "ID"
sor_key_column2 = "Code"
mapping_key_column1 = "ID"
mapping_key_column2 = "Code"

# Columns to copy from SOR or SOR_OTHER
sor_value_columns = ["Value1", "Value2", "Value3"]  # Replace with actual column names

# Load Excel files
df_sor = pd.read_excel(sor_file, dtype=str)
df_sor_other = pd.read_excel(sor_other_file, dtype=str)
df_mapping = pd.read_excel(mapping_file, dtype=str)

# Ensure column names are clean
df_sor.columns = df_sor.columns.str.strip()
df_sor_other.columns = df_sor_other.columns.str.strip()
df_mapping.columns = df_mapping.columns.str.strip()

# Check if required columns exist in both SOR and SOR_OTHER files
required_columns_sor = {"loannumber", "department", "org"}.union(sor_value_columns)
required_columns_mapping = {mapping_key_column1, mapping_key_column2}

if not required_columns_sor.issubset(df_sor.columns):
    raise ValueError(f"Missing required columns in SOR file: {required_columns_sor - set(df_sor.columns)}")

if not required_columns_mapping.issubset(df_mapping.columns):
    raise ValueError(f"Missing columns in Mapping file: {required_columns_mapping - set(df_mapping.columns)}")

# Convert key columns to string for consistent matching
df_sor["loannumber"] = df_sor["loannumber"].astype(str).str.strip()
df_sor["department"] = df_sor["department"].astype(str).str.strip()
df_sor["org"] = df_sor["org"].astype(str).str.strip()
df_mapping[mapping_key_column1] = df_mapping[mapping_key_column1].astype(str).str.strip()
df_mapping[mapping_key_column2] = df_mapping[mapping_key_column2].astype(str).str.strip()
df_sor_other["internal_id"] = df_sor_other["internal_id"].astype(str).str.strip()
df_sor_other["org"] = df_sor_other["org"].astype(str).str.strip()

# Extract internal_id from loannumber (assuming format: "org+internal_id+department")
df_sor["internal_id"] = df_sor["loannumber"].str.extract(r'(\d+)')  # Extract the numeric part

# Merge Mapping file with SOR file (first match attempt)
df_merged = df_mapping.merge(
    df_sor[["loannumber", "internal_id", "org", "department"] + sor_value_columns],
    how="left",
    left_on=[mapping_key_column1, mapping_key_column2],
    right_on=["loannumber", "department"]
)

# Drop extra key columns from SOR (if needed)
df_merged.drop(columns=["loannumber", "department"], inplace=True)

# Merge with SOR_OTHER using extracted internal_id and org
df_merged = df_merged.merge(
    df_sor_other[["internal_id", "org"] + sor_value_columns],
    how="left",
    on=["internal_id", "org"],
    suffixes=("", "_other")
)

# Fill missing values with SOR_OTHER values
for column in sor_value_columns:
    df_merged[column] = df_merged[column].combine_first(df_merged[column + "_other"])
    df_merged.drop(columns=[column + "_other"], inplace=True)  # Cleanup extra columns

# Save the updated data to a new Excel file
df_merged.to_excel(output_file, index=False)

print(f"Updated data saved in {output_file}")
