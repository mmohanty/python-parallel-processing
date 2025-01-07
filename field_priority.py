import pandas as pd

# Load the Excel file
file_path = "test_file.xlsx"
xls = pd.ExcelFile(file_path)

# Read sheets
file_mapping_df = pd.read_excel(xls, sheet_name='FileMapping')  # Sheet containing filename and fileId
lob_df = pd.read_excel(xls, sheet_name='L_Data')  # Sheet containing LOB1, LOB2, LOB3
priority_df = pd.read_excel(xls, sheet_name='PriorityData')  # Sheet containing element priority


# Function to get LOB values and priority
def get_priority_for_element(file_name, element_name):
    # Get the fileId based on the filename
    file_id = file_mapping_df.loc[file_mapping_df['filename'] == file_name, 'fileId'].values[0]

    # Filter LOB1, LOB2, LOB3 based on fileId
    lob_data_filtered = lob_df[lob_df['fileId'] == file_id][['LOB1', 'LOB2', 'LOB3']]

    print(f"Filtered LOB data for file '{file_name}':\n{lob_data_filtered}\n")

    # Get the priority values for the element for different document types
    priority_filtered = priority_df[(priority_df['fileId'] == file_id) & (priority_df['element'] == element_name)]

    print(f"Priority values for element '{element_name}' in file '{file_name}':\n{priority_filtered}\n")

    return lob_data_filtered, priority_filtered


# Example usage
filename = "file1.xlsx"
element_name = "Element1"
lob_data, priority_data = get_priority_for_element(filename, element_name)

print(lob_data)
print(priority_data)

