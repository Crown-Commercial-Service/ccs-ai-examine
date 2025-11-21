import pandas as pd
import numpy as np
import random
import os

def generate_dummy_contracts_data(num_rows=30):
    """
    Generates a dummy pandas DataFrame mimicking the structure of the
    contracts data from Tussell, including specified missing values.
    """
    columns = [
        'CustomerName', 'SupplierName', 'SupplierCompanyRegistrationNumber',
        'Contract Start Date', 'Contract End Date', 'Contract Duration (Months)',
        'Contract Title', 'Contract Description', 'Total Contract Value - Low (GBP)',
        'Total Contract Value - High (GBP)'
    ]

    data = {}
    for col in columns:
        data[col] = [f"{col}_{i}" for i in range(num_rows)]

    df = pd.DataFrame(data)

    # Randomly generate start dates and contract lengths
    df['Contract Start Date'] = pd.to_datetime('2024-01-01') + pd.to_timedelta(np.arange(num_rows), unit='D')
    df['Contract Duration (Months)'] = np.random.randint(12, 60, num_rows)
    # Generate end dates to be consistent with start and length
    df['Contract End Date'] = df['Contract Start Date'] + pd.to_timedelta(df['Contract Duration (Months)']*30, unit='D')

    # Convert numeric columns
    df['Total Contract Value - Low (GBP)'] = np.random.randint(10000, 1000000, num_rows)
    df['Total Contract Value - High (GBP)'] = df['Total Contract Value - Low (GBP)'] + np.random.randint(5000, 50000, num_rows)

    # Ensure 'Supplier Company Registration Number' is unique for joining purposes
    df['SupplierCompanyRegistrationNumber'] = [f"REG{1000 + i}" for i in range(num_rows)]

    # Introduce 5 rows with one missing value
    for i in range(5):
        row_idx = random.randint(0, num_rows - 1)
        col_to_nan = random.choice(columns)
        df.loc[row_idx, col_to_nan] = np.nan

    # Introduce 1 row with two missing values
    row_idx_two_nan = random.randint(0, num_rows - 1)
    col_to_nan_1 = random.choice(columns)
    col_to_nan_2 = random.choice([col for col in columns if col != col_to_nan_1])
    df.loc[row_idx_two_nan, col_to_nan_1] = np.nan
    df.loc[row_idx_two_nan, col_to_nan_2] = np.nan

    return df

def generate_dummy_mi_data(num_rows=30):
    """
    Generates a dummy pandas DataFrame mimicking the structure of the
    MI data, including specified missing values.
    """
    columns = [
        'SupplierName', 'SupplierKey', 'CustomerName', 'FinancialYear',
        'FinancialMonth', 'EvidencedSpend'
    ]
    # first we make a top half of the table with simulated numbers
    num_rows_first_half = round(num_rows/2)
    data_first_half = {}
    for col in columns:
        data_first_half[col] = [f"{col}_{i}" for i in range(num_rows_first_half)]
    df_first_half = pd.DataFrame(data_first_half)
    # Populate with realistic data types
    df_first_half['FinancialYear'] = np.random.randint(2020, 2024, num_rows_first_half)
    df_first_half['FinancialMonth'] = np.random.randint(1, 13, num_rows_first_half)
    df_first_half['EvidencedSpend'] = np.random.uniform(100, 5000, num_rows_first_half).round(2)
    df_first_half['SupplierKey'] = [f"KEY{100 + i}" for i in range(num_rows_first_half)]

    # then we create the second half of the table by moving each entry back by one year, and halving the amount
    df_second_half = df_first_half.copy()
    df_second_half['FinancialYear'] = df_second_half['FinancialYear'] - 1
    df_second_half['EvidencedSpend'] = df_second_half['EvidencedSpend'] / 2

    # combine first and second halves together to get whole dataset, and remove any rows that exceed the row limit
    df = pd.concat([df_first_half, df_second_half])

    # Introduce 5 rows with one missing value
    for i in range(5):
        row_idx = random.randint(0, num_rows - 1)
        col_to_nan = random.choice(columns)
        df.loc[row_idx, col_to_nan] = np.nan

    # Introduce 1 row with two missing values
    row_idx_two_nan = random.randint(0, num_rows - 1)
    col_to_nan_1 = random.choice(columns)
    col_to_nan_2 = random.choice([col for col in columns if col != col_to_nan_1])
    df.loc[row_idx_two_nan, col_to_nan_1] = np.nan
    df.loc[row_idx_two_nan, col_to_nan_2] = np.nan

    return df

def generate_dummy_reg_key_pairs(contracts_df, mi_df):
    """
    Generates a dummy DataFrame of SupplierKey and CompanyRegistrationNumber pairs.
    """
    # Get the relevant columns from the input dataframes
    contracts_keys = contracts_df[['SupplierCompanyRegistrationNumber']]
    mi_keys = mi_df[['SupplierKey']]

    # Create a combined dataframe to ensure some matching keys
    # We'll take a subset of each to ensure not all keys from one are in the other
    combined = pd.concat([
        contracts_keys.head(20),
        mi_keys.head(20).rename(columns={'SupplierKey': 'SupplierCompanyRegistrationNumber'}) # Just for concat
    ]).dropna().drop_duplicates()

    # Create the pairs
    reg_key_pairs = pd.DataFrame({
        'SupplierCompanyRegistrationNumber': combined['SupplierCompanyRegistrationNumber'],
        'SupplierKey': [f"KEY{100 + i}" for i in range(len(combined))]
    })

    # Add some extra pairs that are not in the other datasets
    extra_pairs = pd.DataFrame({
        'SupplierCompanyRegistrationNumber': [f"REG{2000 + i}" for i in range(10)],
        'SupplierKey': [f"KEY{200 + i}" for i in range(10)]
    })

    final_df = pd.concat([reg_key_pairs, extra_pairs], ignore_index=True)
    return final_df

if __name__ == "__main__":
    # --- Generate and save contracts data ---
    dummy_contracts = generate_dummy_contracts_data()
    print("--- Generated Dummy Contracts Data ---")
    print(dummy_contracts.head())
    print(f"\nTotal rows: {len(dummy_contracts)}")
    print(f"Number of missing values per column:\n{dummy_contracts.isnull().sum()}")
    print(f"Total missing values: {dummy_contracts.isnull().sum().sum()}")

    output_dir = 'dummy_data'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    contracts_output_path = os.path.join(output_dir, 'dummy_contracts.csv')
    dummy_contracts.to_csv(contracts_output_path, index=False)
    print(f"\nSuccessfully saved dummy contracts data to {contracts_output_path}")

    # --- Generate and save MI data ---
    dummy_mi = generate_dummy_mi_data()
    print("\n--- Generated Dummy MI Data ---")
    print(dummy_mi.head())
    print(f"\nTotal rows: {len(dummy_mi)}")
    print(f"Number of missing values per column:\n{dummy_mi.isnull().sum()}")
    print(f"Total missing values: {dummy_mi.isnull().sum().sum()}")

    mi_output_path = os.path.join(output_dir, 'dummy_mi.csv')
    dummy_mi.to_csv(mi_output_path, index=False)
    print(f"\nSuccessfully saved dummy MI data to {mi_output_path}")

    # --- Generate and save reg key pairs ---
    dummy_reg_key = generate_dummy_reg_key_pairs(dummy_contracts, dummy_mi)
    print("\n--- Generated Dummy Reg Key Pairs ---")
    print(dummy_reg_key.head())
    print(f"\nTotal rows: {len(dummy_reg_key)}")
    
    reg_key_output_path = os.path.join(output_dir, 'dummy_reg_key_pairs.csv')
    dummy_reg_key.to_csv(reg_key_output_path, index=False)
    print(f"\nSuccessfully saved dummy reg key pairs data to {reg_key_output_path}")
