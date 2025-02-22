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
    mapping_df = pd.read_excel(mapping_file, dtype={'ID': str})
    sor_df = pd.read_excel(sor_file, dtype={'ID': str})

    # Normalize column names
    sor_df = sor_df.rename(columns={col.strip(): col.strip() for col in sor_df.columns})

    # Merge with SOR data on ID
    merged_df = mapping_df.merge(sor_df[['ID', 'Value1', 'Value2', 'Value3']], on='ID', how='left')

    # Save intermediate result
    merged_df.to_excel(intermediate_file, index=False, engine='openpyxl')
    print(f"Intermediate file saved to {intermediate_file}")


def merge_with_sor_other(intermediate_file, sor_other_file, output_file):
    # Load intermediate and SOR_OTHER data
    merged_df = pd.read_excel(intermediate_file, dtype={'Internal_id': str, 'ORG': str})
    sor_other_df = pd.read_excel(sor_other_file, dtype={'internal_id': str, 'org': str})

    # Normalize column names and strip spaces
    sor_other_df['internal_id'] = sor_other_df['internal_id'].astype(str).str.strip()
    sor_other_df['org'] = sor_other_df['org'].astype(str).str.strip()

    merged_df['Internal_id'] = merged_df['Internal_id'].astype(str).str.strip()
    merged_df['ORG'] = merged_df['ORG'].astype(str).str.strip()

    # Debugging print statements
    missing_values = merged_df['Value1'].isna()
    print("\nRows with missing values before merge:")
    print(merged_df[missing_values][['Internal_id', 'ORG']])

    print("\nSOR_OTHER Data (Before Merge):")
    print(sor_other_df[['internal_id', 'org', 'Value1', 'Value2', 'Value3']])

    # Merge with SOR_OTHER data based on internal_id and org where values are missing
    merged_df_missing = merged_df[missing_values].copy()

    merged_result = merged_df_missing.merge(
        sor_other_df,
        left_on=['Internal_id', 'ORG'],
        right_on=['internal_id', 'org'],
        how='left'
    )

    # Debug: Print result after merge
    print("\nMerged Result After Join:")
    print(merged_result)

    # Rename the correct columns for updating
    merged_result = merged_result.rename(columns={
        'Value1_y': 'Value1',
        'Value2_y': 'Value2',
        'Value3_y': 'Value3'
    })

    # Ensure merged_result has the correct shape
    updated_values = merged_result[['Value1', 'Value2', 'Value3']].reset_index(drop=True)

    # If row counts do not match, fill with empty values
    if len(updated_values) != missing_values.sum():
        print("\nWARNING: Row count mismatch during assignment! Filling missing values with empty strings.")
        expected_rows = missing_values.sum()
        while len(updated_values) < expected_rows:
            updated_values = updated_values.append(pd.Series(["", "", ""], index=['Value1', 'Value2', 'Value3']),
                                                   ignore_index=True)

    # Assign only missing values
    merged_df.loc[missing_values, ['Value1', 'Value2', 'Value3']] = updated_values.values

    # Save final result
    merged_df.to_excel(output_file, index=False, engine='openpyxl')
    print(f"\nFinal merged file saved to {output_file}")


if __name__ == '__main__':

    # Define file paths
    sor_file1= "data/xls/sor.xlsx"  # Main SOR file
    sor_other_file1 = "data/xls/sor_other.xlsx"  # Another source for missing values
    mapping_file1 = "data/xls/mapping.xlsx"  # Mapping file
    output_file1 = "data/xls/updated_mapping.xlsx"  # Final output file

    intermediate_file1 = 'data/xls/intermediate.xlsx'
    merge_with_sor(mapping_file1, sor_file1, intermediate_file1)
    merge_with_sor_other(intermediate_file1, sor_other_file1, output_file1)


