import pandas as pd
import json

# Load the Excel file
file_path = "test_file.xlsx"
xls = pd.ExcelFile(file_path)

# Read sheets
file_mapping_df = pd.read_excel(xls, sheet_name='FileMapping')  # Sheet containing filename and fileId
lob_df = pd.read_excel(xls, sheet_name='L_Data')  # Sheet containing LOB1, LOB2, LOB3
priority_df = pd.read_excel(xls, sheet_name='PriorityData')  # Sheet containing element priority


# Function to get matched elements based on priority
def get_matched_elements_by_priority(data, file_mapping_df, lob_df, priority_df):
    matched_elements = []  # Store matched elements

    # Group JSON data by (file_name, element_name)
    grouped_data = {}
    for record in data:
        key = (record['file_name'], record['element_name'])
        if key not in grouped_data:
            grouped_data[key] = []
        grouped_data[key].append(record)

    for (file_name, element_name), records in grouped_data.items():
        # Get fileId based on file_name from FileMapping sheet
        file_id = file_mapping_df.loc[file_mapping_df['filename'] == file_name, 'fileId']
        if file_id.empty:
            print(f"No fileId found for file '{file_name}'")
            continue
        file_id = file_id.values[0]

        # Get LOB1, LOB2, LOB3 based on fileId from LOBData sheet
        lob_info = lob_df[lob_df['fileId'] == file_id][['LOB1', 'LOB2', 'LOB3']]
        if lob_info.empty:
            print(f"No LOB data found for fileId '{file_id}'")
            continue

        lob1, lob2, lob3 = lob_info.iloc[0].values

        # Filter priority data based on LOB1, LOB2, LOB3
        priority_filtered = priority_df[
            (priority_df['LOB1'] == lob1) &
            (priority_df['LOB2'] == lob2) &
            (priority_df['LOB3'] == lob3)
        ]

        # Create a priority map for document types
        priority_list = priority_filtered[['documentType', 'priority']].to_dict(orient='records')

        # Sort records by priority based on document type
        records_sorted_by_priority = sorted(
            records,
            key=lambda x: next(
                (p['priority'] for p in priority_list if p['documentType'] == x['document_type']),
                float('inf')  # If document_type not found, move to the end
            )
        )

        # If priority list is not empty, get the highest-priority record
        if records_sorted_by_priority:
            highest_priority_record = records_sorted_by_priority[0]
            matched_elements.append(highest_priority_record)
            print(f"Matched Element: File: '{file_name}', Element: '{element_name}' → Normal Value: {highest_priority_record['normal_value']}")

    return matched_elements


# Example usage

# Sample JSON input
json_input = '''
[
    {"element_name": "Element1", "element_value": "abc", "file_name": "file1.xlsx", "document_type": "Type1", "normal_value": "abc"},
    {"element_name": "Element1", "element_value": "abcd", "file_name": "file1.xlsx", "document_type": "Type2", "normal_value": "abcd"},
    {"element_name": "Element2", "element_value": "xyz", "file_name": "file1.xlsx", "document_type": "Type1", "normal_value": "xyz_value"},
    {"element_name": "Element2", "element_value": "xyz_alt", "file_name": "file2.xlsx", "document_type": "Type3", "normal_value": "alt_value"},
    {"element_name": "Element1", "element_value": "pqr", "file_name": "file2.xlsx", "document_type": "Type3", "normal_value": "pqr_value"}
]
'''
# Convert JSON string to Python list of dictionaries
data = json.loads(json_input)
# Run the function
matched_elements = get_matched_elements_by_priority(data, file_mapping_df, lob_df, priority_df)

# Print the final list of matched elements
print("\nMatched Elements List:")
print(json.dumps(matched_elements, indent=4))