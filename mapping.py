import pandas as pd

import pandas as pd


# def merge_excel_files(mapping_file, sor_file, sor_other_file, output_file):
#     # Load the Excel files
#     mapping_df = pd.read_excel(mapping_file)
#     sor_df = pd.read_excel(sor_file)
#     sor_other_df = pd.read_excel(sor_other_file)
#
#     # Merge with SOR data on ID
#     merged_df = mapping_df.copy()
#     merged_df = merged_df.merge(sor_df[['ID', 'Value1', 'Value2', 'Value3']], on='ID', how='left')
#
#     # Identify rows where Value1, Value2, Value3 are still missing
#     missing_values = merged_df['Value1'].isna()
#
#     # Merge with SOR_OTHER data based on internal_id and org where values are missing
#     updated_values = mapping_df[missing_values].merge(
#         sor_other_df, left_on=['Internal_id', 'ORG'], right_on=['internal_id', 'org'], how='left'
#     )[['Value1', 'Value2', 'Value3']]
#
#     # Assign only missing values without changing other columns
#     merged_df.loc[missing_values, ['Value1', 'Value2', 'Value3']] = updated_values.values
#
#     # Save to a new Excel file
#     merged_df.to_excel(output_file, index=False, engine='openpyxl')
#     print(f"Merged file saved to {output_file}")


def merge_with_sor(mapping_file, sor_file, intermediate_file):
    # Load mapping and SOR data
    mapping_df = pd.read_excel(mapping_file)
    sor_df = pd.read_excel(sor_file)

    # Ensure columns exist before merging
    expected_cols = {'ID', 'Value1', 'Value2', 'Value3'}
    available_cols = set(sor_df.columns)

    # Rename columns to match expected names if needed
    sor_df = sor_df.rename(columns={col: col.strip() for col in sor_df.columns})

    # Fill missing columns with NaN if they don't exist
    for col in expected_cols - available_cols:
        sor_df[col] = pd.NA

    # Merge with SOR data on ID
    merged_df = mapping_df.merge(sor_df[['ID', 'Value1', 'Value2', 'Value3']], on='ID', how='left')

    # Save intermediate result
    merged_df.to_excel(intermediate_file, index=False, engine='openpyxl')
    print(f"Intermediate file saved to {intermediate_file}")

def merge_with_sor_other(intermediate_file, sor_other_file, output_file):
    # Load intermediate and SOR_OTHER data
    merged_df = pd.read_excel(intermediate_file)
    sor_other_df = pd.read_excel(sor_other_file)

    # Ensure column consistency
    sor_other_df = sor_other_df.rename(columns={col: col.strip() for col in sor_other_df.columns})

    # Identify rows where Value1, Value2, Value3 are still missing
    missing_values = merged_df['Value1'].isna()

    # Merge with SOR_OTHER data based on internal_id and org where values are missing
    updated_values = merged_df[missing_values].merge(
        sor_other_df, left_on=['Internal_id', 'ORG'], right_on=['internal_id', 'org'], how='left'
    )[['Value1', 'Value2', 'Value3']]

    # Assign only missing values without changing other columns
    merged_df.loc[missing_values, ['Value1', 'Value2', 'Value3']] = updated_values.values

    # Save final result
    merged_df.to_excel(output_file, index=False, engine='openpyxl')
    print(f"Final merged file saved to {output_file}")


if __name__ == '__main__':

    # Define file paths
    sor_file = "data/xls/sor.xlsx"  # Main SOR file
    sor_other_file = "data/xls/sor_other.xlsx"  # Another source for missing values
    mapping_file = "data/xls/mapping.xlsx"  # Mapping file
    output_file = "data/xls/updated_mapping.xlsx"  # Final output file

    intermediate_file = 'intermediate.xlsx'
    merge_with_sor(mapping_file, sor_file, intermediate_file)
    merge_with_sor_other(intermediate_file, sor_other_file, output_file)


