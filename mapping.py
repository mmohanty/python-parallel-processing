import pandas as pd

# Define file paths
sor_file = "sor.xlsx"  # Source Excel file
mapping_file = "mapping.xlsx"  # Mapping Excel file
output_file = "updated_mapping.xlsx"  # New output file

# Define columns for matching
sor_key_column1 = "ID"   # First column for matching in SOR
sor_key_column2 = "Code"  # Second column for matching in SOR
mapping_key_column1 = "ID"  # First column for matching in Mapping file
mapping_key_column2 = "Code"  # Second column for matching in Mapping file

# Columns to copy from SOR to Mapping file
sor_value_columns = ["Value1", "Value2", "Value3"]  # Replace with actual column names

# Load Excel files
df_sor = pd.read_excel(sor_file, dtype=str)  # Read everything as strings
df_mapping = pd.read_excel(mapping_file, dtype=str)

# Ensure column names are clean
df_sor.columns = df_sor.columns.str.strip()
df_mapping.columns = df_mapping.columns.str.strip()

# Check if required columns exist in both files
required_columns_sor = {sor_key_column1, sor_key_column2}.union(sor_value_columns)
required_columns_mapping = {mapping_key_column1, mapping_key_column2}

if not required_columns_sor.issubset(df_sor.columns):
    raise ValueError(f"Missing columns in SOR file: {required_columns_sor - set(df_sor.columns)}")

if not required_columns_mapping.issubset(df_mapping.columns):
    raise ValueError(f"Missing columns in Mapping file: {required_columns_mapping - set(df_mapping.columns)}")

# Convert key columns to string for consistent matching
df_sor[sor_key_column1] = df_sor[sor_key_column1].astype(str).str.strip()
df_sor[sor_key_column2] = df_sor[sor_key_column2].astype(str).str.strip()
df_mapping[mapping_key_column1] = df_mapping[mapping_key_column1].astype(str).str.strip()
df_mapping[mapping_key_column2] = df_mapping[mapping_key_column2].astype(str).str.strip()

# Merge Mapping file with SOR file based on the matching columns
df_merged = df_mapping.merge(
    df_sor[[sor_key_column1, sor_key_column2] + sor_value_columns],
    how="left",  # Keeps all rows from df_mapping
    left_on=[mapping_key_column1, mapping_key_column2],
    right_on=[sor_key_column1, sor_key_column2]
)

# Drop extra key columns from SOR (if needed)
df_merged.drop(columns=[sor_key_column1, sor_key_column2], inplace=True)

# Save the updated data to a new Excel file
df_merged.to_excel(output_file, index=False)

print(f"Updated data saved in {output_file}")
