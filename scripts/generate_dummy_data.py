import pandas as pd
import numpy as np
import random
import os

def generate_dummy_contracts_data():
    """
    Generates a dummy pandas DataFrame mimicking the structure of the contracts data from Tussell.
    Row 1 = a contract which has expired
    Rows 2-3 = contracts which are still live and have been running for >1 year
    Row 4 = a contract which is still live and has been running for <1 year
    Row 5 = a contract which has no corresponding MI, and should be retained in the end data
    Row 6 = a more recent contract between the buyer and supplier of row 1, to test whether anything is being double-counted
    Row 7 = Edge Case - Contract with Incorrect buyer name "Buyer C LTD" for Supplier 6 
    """
    data = {
        'buyer': ['Buyer A', 'Buyer B', 'Buyer C Limited','Department for Work and Pensions', 'Buyer no MI','Buyer A','Buyer C LTD'],
        'suppliers': ['Supplier 1','Supplier 2','Supplier 3', 'Supplier 1', 'Supplier no MI','Supplier 1','Supplier 6'],
        'SupplierCompanyRegistrationNumber': [1001, 1002, 1003, 1001, 5678, 1001,1006],
        'contract_start': [pd.to_datetime(i) for i in ['2024-04-01', '2024-04-01', '2024-10-01', '2025-11-01', '2025-12-01', '2026-01-01','2025-06-01']],
        'contract_end': [pd.to_datetime(i) for i in ['2025-04-01', '2027-04-01', '2027-10-01', '2028-04-01', '2027-05-01', '2026-07-01','2027-06-01']],
        'contract_months': [12, 36, 36, 36, 24, 6, 24],
        'contract_title': [f"Contract {i+1}" for i in range(7)],
        'contract_description': [ f"Description for contract {i+1}, with commas that need to be handled when parsing" for i in range(7)],
        'award_value': [ 1e6, 2.5e6, 5e6, 7.5e6, 10e6, 1e6, 3e6]
    }
    df = pd.DataFrame(data)
    # add extra cols for metadata that isn't relevant to spend calc
    df['framework_title'] = 'RM1'
    df['source'] = 'Online'
    df['awarded'] = df['contract_start']
    df['latest_employees'] = 10
    return df

def generate_dummy_mi_data():
    """
    Generates a dummy pandas DataFrame mimicking the structure of the MI data.
    Rows 1-4 = one exemplary MI entry each for the contracts in the dummy contract dataset
    Row 5 = an MI entry for the first contract, but with its SupplierKey as a float, to check key conversion works as expected
    Row 6 = an MI entry for the second contract where the buyer name has been capitalised
    Row 7 = an MI entry for the third contract where the buyer name has been contracted
    Row 8 = an MI entry for the fourth contract where the buyer name acronym has been used
    Rows 9-10: MI entries for contracts which aren't in the dummy contracts dataset
    Row 11: an MI entry for a contract which isn't in the dummy contracts dataset, and which is missing its SupplierKey
    Row 12: an MI entry for a contract where both the buyer and supplier are in the contracts dataset under consistent names, but there isn't a contract between them in the contracts dataset
    Row 13: an MI entry for the third contract where the buyer name has been contracted, and the SupplierKey is a float
    """
    data = {
        'SupplierName': ['Supplier 1', 'Supplier 2', 'Supplier 3', 'Supplier 1',
                        'Supplier 1', 'Supplier 2', 'Supplier 3', 'Supplier 1',
                        'Supplier 99', 'Supplier 100', 'Supplier 101', 'Supplier 1',
                         'Supplier 3'],
        'SupplierKey': ['1', '2', '3', '1.0', '1', '2', '3', '1','99', '100', np.nan, '1','3.0'],
        'CustomerName': ['Buyer A', 'Buyer B', 'Buyer C Limited', 'Department for Work and Pensions',
                         'Buyer A', 'BUYER B', 'Buyer C LTD', 'DWP',
                         'Buyer Y', 'Buyer Z', 'Buyer Z', 'Buyer C Limited',
                         'Buyer C LTD',],
        'FinancialYear': [2024 for i in range(13)],
        'FinancialMonth': range(0, 13, 1),
        'EvidencedSpend': [1e5 for i in range(13)]
    }
    # ensure that SupplierKey mixed types are preserved 
    df = pd.DataFrame(data).astype({'SupplierKey': 'object'})
    return df

def generate_dummy_reg_key_pairs():
    """
    Generates a dummy DataFrame of SupplierKey and CompanyRegistrationNumber pairs.
    """
    data = {
        'SupplierCompanyRegistrationNumber': [1001, 1002, 1003, 1099, 1100, 5678, 1006],
        'SupplierKey': [1, 2, 3, 99, 100, 4678, 6]
    }
    df = pd.DataFrame(data)
    return df

if __name__ == "__main__":
    # --- Generate and save contracts data ---
    dummy_contracts = generate_dummy_contracts_data()
    print("--- Generated Dummy Contracts Data ---")
    print(dummy_contracts.head())
    print(f"\nTotal rows: {len(dummy_contracts)}")
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
    mi_output_path = os.path.join(output_dir, 'dummy_mi.csv')
    dummy_mi.to_csv(mi_output_path, index=False)
    print(f"\nSuccessfully saved dummy MI data to {mi_output_path}")

    # --- Generate and save reg key pairs ---
    dummy_reg_key = generate_dummy_reg_key_pairs()
    print("\n--- Generated Dummy Reg Key Pairs ---")
    print(dummy_reg_key.head())
    print(f"\nTotal rows: {len(dummy_reg_key)}")
    reg_key_output_path = os.path.join(output_dir, 'dummy_reg_key_pairs.csv')
    dummy_reg_key.to_csv(reg_key_output_path, index=False)
    print(f"\nSuccessfully saved dummy reg key pairs data to {reg_key_output_path}")