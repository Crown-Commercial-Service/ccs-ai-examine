import os
import argparse
import pandas as pd
import numpy as np
import random
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

def get_live_data(outdir: str):
    ## STEP 1: GET CONTRACT DETAILS FROM TUSSELL DATA
    # connect to db using creds
    conn_string = '{}://{}:{}@{}:{}/{}?driver={}'.format(
        os.getenv("DB_TYPE"),
        os.getenv("DB_USER"),
        os.getenv("DB_PWD"),
        os.getenv("DB_SERVER"),
        os.getenv("DB_PORT"),
        os.getenv("DB_NAME_TUSSELL"),
        os.getenv("DB_DRIVER")
    )
    engine = create_engine(conn_string)
    conn = engine.connect()
    # find GCloud 10-14 contract details
    # note that we join the Company Registration Number from a separate table, and only keep contract entries where a match is found
    # (because these are the only ones that we can link into MI data)
    contracts_query = """
        SELECT
            t1.awarded,
            t1.buyer,
            t1.suppliers,
            t1.award_value,
            t1.contract_start,
            t1.contract_end,
            t1.contract_months,
            t1.contract_title,
            t1.contract_description,
            t1.framework_title,
            t1.source,
            t1.supplier_ids,
            t2.id AS supplier_id,
            t2.company_number,
            t2.latest_employees
        FROM dbo.Tussell_ContractAwards_API t1
        CROSS APPLY OPENJSON(t1.supplier_ids) AS j
        INNER JOIN dbo.Tussell_Suppliers_API t2 
            ON CAST(j.value AS INT) = t2.id
        WHERE (
                t1.framework_title LIKE 'RM1557.10%'
                OR t1.framework_title LIKE 'RM1557.11%'
                OR t1.framework_title LIKE 'RM1557.12%'
                OR t1.framework_title LIKE 'RM1557.13%'
                OR t1.framework_title LIKE 'RM1557.14%'
            )
    """
    contracts = pd.read_sql(contracts_query, conn)
    contracts = contracts.rename(columns={'company_number': 'SupplierCompanyRegistrationNumber'})
    print("Contracts parsed")

    # Save contracts DataFrame to CSV
    contracts.to_csv(os.path.join(outdir, 'contracts.csv'), index=False)
    print(f"Saved contracts data to {os.path.join(outdir, 'contracts.csv')}")

    ## STEP 2: GET MI DATA
    # connect to db using creds
    conn_string = '{}://{}:{}@{}:{}/{}?driver={}'.format(
        os.getenv("DB_TYPE"),
        os.getenv("DB_USER"),
        os.getenv("DB_PWD"),
        os.getenv("DB_SERVER"),
        os.getenv("DB_PORT"),
        os.getenv("DB_NAME_MI"),
        os.getenv("DB_DRIVER")
    )
    engine = create_engine(conn_string)
    conn = engine.connect()

    MI_query = f"""
            SELECT SupplierName,SupplierKey,CustomerName,[Group],FinancialYear,FinancialMonth,EvidencedSpend FROM dbo.AggregatedSpendReporting
            WHERE FrameworkName LIKE 'G-Cloud 1%'
        """
    GCloud_MI = pd.read_sql(MI_query, conn)
    GCloud_MI = GCloud_MI.rename(columns={'Group': 'CustomerGroup'})
    print("MI parsed")

    # Save MI entries to CSV
    GCloud_MI.to_csv(os.path.join(outdir, 'mi.csv'), index=False)
    print(f"Saved MI data to {os.path.join(outdir, 'mi.csv')}")

    ## STEP 3: GET COMPANY REGISTRATION NUMBER - SUPPLIER KEY PAIRS
    # connect to db using creds
    conn_string = '{}://{}:{}@{}:{}/{}?driver={}'.format(
        os.getenv("DB_TYPE"),
        os.getenv("DB_USER"),
        os.getenv("DB_PWD"),
        os.getenv("DB_SERVER"),
        os.getenv("DB_PORT"),
        os.getenv("DB_NAME_REG"),
        os.getenv("DB_DRIVER")
    )
    engine = create_engine(conn_string)
    conn = engine.connect()
    # find supplier Company Registration Number and CCS SupplierKey, to join Tussell to MI data
    # also take supplier status
    reg_number_supplier_key_query = """
        SELECT SupplierKey,CompanyRegistrationNumber FROM sf.Attributes_sf_vw_Suppliers
    """
    reg_number_supplier_key = pd.read_sql(reg_number_supplier_key_query, conn)
    reg_number_supplier_key = reg_number_supplier_key.rename(columns={'CompanyRegistrationNumber': 'SupplierCompanyRegistrationNumber'})
    print("Company Registration Numbers parsed")

    # Save reg numbers DataFrame to CSV
    reg_number_supplier_key.to_csv(os.path.join(outdir, 'reg_number_supplier_key.csv'), index=False)
    print(f"Saved reg number data to {os.path.join(outdir, 'reg_number_supplier_key.csv')}")

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
        'EvidencedSpend': [1e5 for i in range(13)],
        'CustomerGroup': ['Unknown' for _ in range(13)] # Added to keep dummy MI schema consistent with live MI for downstream summarise step
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

def get_dummy_data(outdir: str):
    contracts = generate_dummy_contracts_data()
    mi = generate_dummy_mi_data()
    reg = generate_dummy_reg_key_pairs()

    contracts.to_csv(os.path.join(outdir, 'contracts.csv'), index=False)
    mi.to_csv(os.path.join(outdir, 'mi.csv'), index=False)
    reg.to_csv(os.path.join(outdir, 'reg_number_supplier_key.csv'), index=False)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["dummy", "live"], required=True)
    parser.add_argument("--outdir", required=True)
    args = parser.parse_args()

    if not os.path.exists(args.outdir):
        os.makedirs(args.outdir)

    if args.mode == "live":
        get_live_data(args.outdir)
    else:
        get_dummy_data(args.outdir)

if __name__ == "__main__":
    main()