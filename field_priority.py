import pandas as pd
import json
import os

# Load the Excel file
file_path = "test_file.xlsx"
xls = pd.ExcelFile(file_path)

# Read sheets
lob_df = pd.read_excel(xls, sheet_name='L_Data')  # Contains fileId, LOB1, LOB2, LOB3
priority_df = pd.read_excel(xls, sheet_name='PriorityData')  # Contains LOB1, LOB2, LOB3, element, documentType, priority


# Function to get highest-priority elements across multiple documents
def get_highest_priority_elements(data_list, lob_df, priority_df):
    element_priority_map = {}  # Store the highest-priority element for each `element_name`

    for data in data_list:
        # Extract fileId, document_type, and elements
        file_id = data["fileId"]
        document_type = data["document_type"]
        elements = data["data"]

        # Get LOB1, LOB2, LOB3 based on fileId from LOBData sheet
        lob_info = lob_df[lob_df['fileId'] == int(file_id)][['LOB1', 'LOB2', 'LOB3']]
        if lob_info.empty:
            print(f"No LOB data found for fileId '{file_id}'")
            continue

        lob1, lob2, lob3 = lob_info.iloc[0].values

        for record in elements:
            element_name = record["element_name"]

            # Filter priority data based on LOB1, LOB2, LOB3, element, and documentType
            priority_filtered = priority_df[
                (priority_df['LOB1'] == lob1) &
                (priority_df['LOB2'] == lob2) &
                (priority_df['LOB3'] == lob3) &
                (priority_df['element'] == element_name) &
                (priority_df['documentType'] == document_type)
            ]

            if priority_filtered.empty:
                print(f"No priority data found for element '{element_name}' with documentType '{document_type}' for lob '{lob1}' ")
                continue

            # Get priority rank
            priority_rank = priority_filtered.iloc[0]["priority"]

            # Update the element if it has a higher priority (lower number)
            if element_name not in element_priority_map or element_priority_map[element_name]["priority_rank"] > priority_rank:
                element_priority_map[element_name] = {
                    "fileId": file_id,
                    "file_name": data["file_name"],
                    "document_type": document_type,
                    "element_name": element_name,
                    "element_value": record["element_value"],
                    "normal_value": record["normal_value"],
                    "priority_rank": priority_rank
                }

    return list(element_priority_map.values())


# Example usage

# Sample JSON input list
json1 = '''{
 "fileId": "101",
 "file_name": "file1.xlsx",
 "document_type": "Type1",
 "data": [
    {"element_name": "Element1", "element_value": "abc", "normal_value": "abc"},
    {"element_name": "Element2", "element_value": "xyz", "normal_value": "xyz_value"}
 ]
}'''

json2 = '''{
 "fileId": "102",
 "file_name": "file2.xlsx",
 "document_type": "Type2",
 "data": [
    {"element_name": "Element1", "element_value": "abcd", "normal_value": "abcd"},
    {"element_name": "Element2", "element_value": "xyz_alt", "normal_value": "alt_value"}
 ]
}'''


# Combine the JSON strings into a list of dictionaries
data = [json.loads(json1), json.loads(json2)]

# Run the function
highest_priority_elements = get_highest_priority_elements(data, lob_df, priority_df)

# Print the final list of highest-priority elements
print("\nHighest Priority Elements List:")
print(json.dumps(highest_priority_elements, indent=4, default=str))