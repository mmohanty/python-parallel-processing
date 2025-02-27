import pandas as pd


def merge_sor_data(mapping_file, sor_file, sor_other_file, output_file):
    # Load Excel files
    mapping_df = pd.read_excel(mapping_file)
    sor_df = pd.read_excel(sor_file)
    sor_other_df = pd.read_excel(sor_other_file)

    # Normalize column names (strip whitespace)
    mapping_df.columns = mapping_df.columns.str.strip()
    sor_df.columns = sor_df.columns.str.strip()
    sor_other_df.columns = sor_other_df.columns.str.strip()

    # Create lookup dictionaries for fast retrieval
    sor_lookup = sor_df.set_index(["ID", "name"])[["Value1", "Value2"]].to_dict("index")
    sor_other_lookup = sor_other_df.set_index(["fac_obl_id", "sor", "name"])[["Value1", "Value2"]].to_dict("index")

    # Function to fetch Value1 and Value2
    def get_values(row):
        key = (row["ID"], row["name"])
        if key in sor_lookup:
            return sor_lookup[key]["Value1"], sor_lookup[key]["Value2"]

        key_other = (row["fac_obl_id"], row["sor"], row["name"])
        if key_other in sor_other_lookup:
            return sor_other_lookup[key_other]["Value1"], sor_other_lookup[key_other]["Value2"]

        return "", ""

    # Apply function
    mapping_df[["Value1", "Value2"]] = mapping_df.apply(get_values, axis=1, result_type="expand")

    # Save the updated file
    mapping_df.to_excel(output_file, index=False)
    print(f"Merged data saved to {output_file}")


# Example usage
merge_sor_data("data/xls/mapping.xlsx", "data/xls/sor.xlsx", "data/xls/sor_other.xlsx", "data/xls/updated_mapping.xlsx")
