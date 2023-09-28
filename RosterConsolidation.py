import pandas as pd

def read_csv_and_rename_columns(file_path, column_mapping, source_id):
    """
    Read a CSV file, rename specified columns, and return a Pandas DataFrame.

    Args:
        file_path (str): The path to the CSV file.
        rename_mapping (dict): A dictionary where keys are the current column names and values are the new column names.
        source_id (str): The column name to identify which data source the data was derived from.

    Returns:
        pandas.DataFrame: The DataFrame with renamed columns.
    """
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Rename specified columns
        df.rename(columns=column_mapping, inplace=True)

        # Add ID column for dataframe
        df.insert(0, source_id, df.index+1)

        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def merge_dataframes(df1, df2, on="EDIPI"):
    """
    Merge two pandas DataFrames and return the merged DataFrame.

    Args:
    - df1 (pd.DataFrame): The first DataFrame to be merged.
    - df2 (pd.DataFrame): The second DataFrame to be merged.
    - on (str): Column or index level names to join on.

    Returns:
    - pd.DataFrame: The merged DataFrame.

    Raises:
    - ValueError: If the input DataFrames are not of the correct data type or if
      the dataframes cannot merge for another reason.
    """
    # Check if df1 and df2 are DataFrames
    if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
        raise ValueError("Both df1 and df2 must be pandas DataFrames")

    try:
        # Perform the merge operation
        merged_df = pd.merge(df1, df2, on=on, how="outer")
        return merged_df
    except Exception as e:
        raise ValueError(f"An error occurred during merge: {e}")

# Convert CSV files to dataframes
dtms_df = read_csv_and_rename_columns("Rosters/DTMS.csv", {'Name': 'DTMS_Name'}, "DTMS_ID")
vantage_df = read_csv_and_rename_columns("Rosters/Vantage.csv", {'Name': 'Vantage_Name', 'DoDID':'EDIPI'}, "Vantage_ID")
ippsa_df = read_csv_and_rename_columns("Rosters/Vantage.csv", {'Name': 'IPPSA_Name', 'DOD ID':'EDIPI'}, "IPPSA_ID")
diss_df = read_csv_and_rename_columns("Rosters/DISS.csv", {'SUBJECT NAME': 'DISS_Name'}, "DISS_ID")
perstat_df = read_csv_and_rename_columns("Rosters/PERSTAT.csv", {'Name': 'PERSTAT_Name'}, "PERSTAT_ID")

# Merge individual report together
merged_df = merge_dataframes(dtms_df, vantage_df)
merged_df = merge_dataframes(merged_df, ippsa_df)
merged_df = merge_dataframes(merged_df, diss_df)
merged_df = merge_dataframes(merged_df, perstat_df)

# Save the merged dataframes to a CSV file
merged_df.to_csv("Rosters/Merge_EDIPI.csv", index=None)