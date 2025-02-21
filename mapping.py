import pandas as pd

# Define file paths
sor_file = "sor.xlsx"  # Main SOR file
sor_other_file = "sor_other.xlsx"  # Another source with missing values
mapping_file = "mapping.xlsx"  # Mapping file
selection_file = "selection.xlsx"  # Selected IDs
output_file = "updated_mapping.xlsx"  # Final output file

# Define columns for matching
sor_key_column1 = "ID"   # First column for matching in SOR
sor_key_column2 = "Code"  # Second column for matching in SOR
mapping_key_column1 = "ID"  # First column for matching in Mapping file
mapping_key_column2 = "Code"  # Second column for matching in Mapping file

# Columns to copy from SOR or SOR_OTHER
sor_value_columns = ["Value1", "Value2", "Value3"]  # Replace with actual column names

# Load Excel files
df_sor = pd.read_excel(sor_file, dtype=str)  # Read SOR
df_sor_other = pd.read_excel(sor_other_file, dtype=str)  # Read SOR_OTHER
df_mapping = pd.read_excel(mapping_file, dtype=str)  # Read Mapping file
df_selection = pd.read_excel(selection_file, dtype=str)  # Read selection file

# Ensure column names are clean
df_sor.columns = df_sor.columns.str.strip()
df_sor_other.columns = df_sor_other.columns.str.strip()
df_mapping.columns = df_mapping.columns.str.strip()
df_selection.columns = df_selection.columns.str.strip()

# Ensure required column exists in the selection file
if mapping_key_column1 not in df_selection.columns:
    raise ValueError(f"Missing column {mapping_key_column1} in selection file")

# Filter df_mapping to keep only rows where mapping_key_column1 is in df_selection
df_mapping = df_mapping[df_mapping[mapping_key_column1].isin(df_selection[mapping_key_column1])]

# Check if required columns exist in both SOR and SOR_OTHER files
required_columns_sor = {sor_key_column1, sor_key_column2}.union(sor_value_columns)
required_columns_mapping = {mapping_key_column1, mapping_key_column2}

if not required_columns_sor.issubset(df_sor.columns) and not required_columns_sor.issubset(df_sor_other.columns):
    raise ValueError(f"Missing required columns in both SOR and SOR_OTHER.")

if not required_columns_mapping.issubset(df_mapping.columns):
    raise ValueError(f"Missing columns in Mapping file: {required_columns_mapping - set(df_mapping.columns)}")

# Convert key columns to string for consistent matching
df_sor[sor_key_column1] = df_sor[sor_key_column1].astype(str).str.strip()
df_sor[sor_key_column2] = df_sor[sor_key_column2].astype(str).str.strip()
df_sor_other[sor_key_column1] = df_sor_other[sor_key_column1].astype(str).str.strip()
df_sor_other[sor_key_column2] = df_sor_other[sor_key_column2].astype(str).str.strip()
df_mapping[mapping_key_column1] = df_mapping[mapping_key_column1].astype(str).str.strip()
df_mapping[mapping_key_column2] = df_mapping[mapping_key_column2].astype(str).str.strip()

# Merge Mapping file with SOR file (first match attempt)
df_merged = df_mapping.merge(
    df_sor[[sor_key_column1, sor_key_column2] + sor_value_columns],
    how="left",  # Keeps all rows from df_mapping
    left_on=[mapping_key_column1, mapping_key_column2],
    right_on=[sor_key_column1, sor_key_column2]
)

# Drop extra key columns from SOR (if needed)
df_merged.drop(columns=[sor_key_column1, sor_key_column2], inplace=True)

# Merge with SOR_OTHER only for missing values
df_merged = df_merged.merge(
    df_sor_other[[sor_key_column1, sor_key_column2] + sor_value_columns],
    how="left",
    left_on=[mapping_key_column1, mapping_key_column2],
    right_on=[sor_key_column1, sor_key_column2],
    suffixes=("", "_other")
)

# Fill missing values in df_merged with values from SOR_OTHER
for column in sor_value_columns:
    df_merged[column] = df_merged[column].combine_first(df_merged[column + "_other"])
    df_merged.drop(columns=[column + "_other"], inplace=True)  # Cleanup extra columns

# Save the updated data to a new Excel file
df_merged.to_excel(output_file, index=False)

print(f"Updated data saved in {output_file}")
