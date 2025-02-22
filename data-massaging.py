import pandas as pd

# Load the Excel file
file_path = "your_file.xlsx"  # Replace with your actual file path
df = pd.read_excel(file_path)


# Define transformation function
def transform_values(value1, value2):
    if pd.isna(value1) or pd.isna(value2):  # Handle NaN values
        return value1  # Keep original value if any is missing

    # Example transformation: Concatenating Value1 and Value2 with a suffix
    transformed_value1 = f"{value1}-{value2}-Transformed"

    return transformed_value1


# Apply transformation only if (T_ID is T3 or T7) AND (S_ID is 90)
mask = (df["T_ID"].isin(["T3", "T7"])) & (df["S_ID"] == 90)
df.loc[mask, "Value1"] = df.loc[mask].apply(
    lambda row: transform_values(row["Value1"], row["Value2"]), axis=1
)

# Save the transformed data
output_file = "transformed_file.xlsx"
df.to_excel(output_file, index=False)

print(f"Transformed data saved to {output_file}")
