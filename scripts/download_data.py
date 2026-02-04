import os
import yaml
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

# Read configuration to determine mode
with open('params.yaml', 'r') as f:
    params = yaml.safe_load(f)
data_mode = params['data_mode']
force_erase = params.get('force_erase_live', False)
# Create output directory if it doesn't exist
output_dir = 'data'
os.makedirs(output_dir, exist_ok=True)
stamp_file = os.path.join(output_dir, '.is_live')

# Safety check: Prevent accidental overwrite of live data with dummy or live data with live      
if os.path.exists(stamp_file):
    if data_mode == "dummy":
        msg = "Running DUMMY will erase LIVE data."
    else:
        msg = "Re-running LIVE will overwrite existing LIVE data."

    if not force_erase:
        print("CRITICAL: 'data/' folder currently contains LIVE DATA.")
        print(msg)
        print("ACTION REQUIRED:")
        print("  Set 'force_erase_live: true' in params.yaml to proceed.")
        sys.exit(1)
    else:
        print(" Safety override detected. Proceeding...")
        os.remove(stamp_file)

print(f"Running download_data in {data_mode.upper()} mode")

def generate_dummy_data():
    # Generate dummy contracts, MI, and registration data
    # Contracts data
    """
    Generates a dummy pandas DataFrame mimicking the structure of the contracts data from Tussell.
    Row 1 = a contract which has expired
    Rows 2-3 = contracts which are still live and have been runnning for >1 year
    Row 4 = a contract which is still live and has been running for <1 year
    Row 5 = a contract which has no corresponding MI, and should be retained in the end data
    Row 6 = a more recent contract between the buyer and supplier of row 1, to test whether anything is being double-counted
    """
    contracts_data = {
        'buyer': ['Buyer A', 'Buyer B', 'Buyer C Limited', 'Department for Work and Pensions', 'Buyer no MI', 'Buyer A'],
        'suppliers': ['Supplier 1', 'Supplier 2', 'Supplier 3', 'Supplier 1', 'Supplier no MI', 'Supplier 1'],
        'SupplierCompanyRegistrationNumber': [1001, 1002, 1003, 1001, 5678, 1001],
        'contract_start': [pd.to_datetime(i) for i in ['2024-04-01', '2024-04-01', '2024-10-01', '2025-11-01', '2025-12-01', '2026-01-01']],
        'contract_end': [pd.to_datetime(i) for i in ['2025-04-01', '2027-04-01', '2027-10-01', '2028-04-01', '2027-05-01', '2026-07-01']],
        'contract_months': [12, 36, 36, 36, 24, 6],
        'contract_title': [f"Contract {i+1}" for i in range(6)],
        'contract_description': [f"Description for contract {i+1}, with commas that need to be handled when parsing" for i in range(6)],
        'award_value': [1e6, 2.5e6, 5e6, 7.5e6, 10e6, 1e6]
    }
    contracts = pd.DataFrame(contracts_data)
    # add extra cols for metadata that isn't relevant to spend calc
    contracts['framework_title'] = 'RM1'
    contracts['source'] = 'Online'
    contracts['awarded'] = contracts['contract_start']
    contracts['latest_employees'] = 10    

    # MI data
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
    mi_data = {
        'SupplierName': ['Supplier 1', 'Supplier 2', 'Supplier 3', 'Supplier 1',
                        'Supplier 1', 'Supplier 2', 'Supplier 3', 'Supplier 1',
                        'Supplier 99', 'Supplier 100', 'Supplier 101', 'Supplier 1',
                        'Supplier 3'],
        'SupplierKey': ['1', '2', '3', '1.0', '1', '2', '3', '1', '99', '100', np.nan, '1', '3.0'],
        'CustomerName': ['Buyer A', 'Buyer B', 'Buyer C Limited', 'Department for Work and Pensions',
                        'Buyer A', 'BUYER B', 'Buyer C LTD', 'DWP',
                        'Buyer Y', 'Buyer Z', 'Buyer Z', 'Buyer C Limited',
                        'Buyer C LTD'],
        'FinancialYear': [2024 for i in range(13)],
        'FinancialMonth': range(0,13,1),
        'EvidencedSpend': [1e5 for i in range(13)]
    }
    # ensure that SupplierKey mixed types are preserved 
    GCloud_MI = pd.DataFrame(mi_data).astype({'SupplierKey': 'object'})

    # Reg Keys
    """
    Generates a dummy DataFrame of SupplierKey and CompanyRegistrationNumber pairs.
    """
    reg_data = {
        'SupplierCompanyRegistrationNumber': [1001, 1002, 1003, 1099, 1100, 5678],
        'SupplierKey': [1, 2, 3, 99, 100, 4678]
    }
    reg_number_supplier_key= pd.DataFrame(reg_data)
    
    print("Dummy data generated")
    return contracts, GCloud_MI, reg_number_supplier_key
    
# Comment this block, to test the mock download live data for DVC piepline.
# def download_live_data():
#     ## STEP 1: GET CONTRACT DETAILS FROM TUSSELL DATA
#     # connect to db using creds
#     conn_string = '{}://{}:{}@{}:{}/{}?driver={}'.format(
#         os.getenv("DB_TYPE"),
#         os.getenv("DB_USER"),
#         os.getenv("DB_PWD"),
#         os.getenv("DB_SERVER"),
#         os.getenv("DB_PORT"),
#         os.getenv("DB_NAME_TUSSELL"),
#         os.getenv("DB_DRIVER")
#     )
#     engine = create_engine(conn_string)
#     conn = engine.connect()
#     # find GCloud 10-14 contract details
#     # note that we join the Company Registration Number from a separate table, and only keep contract entries where a match is found
#     # (because these are the only ones that we can link into MI data)
#     contracts_query = """
#         SELECT
#             t1.awarded,
#             t1.buyer,
#             t1.suppliers,
#             t1.award_value,
#             t1.contract_start,
#             t1.contract_end,
#             t1.contract_months,
#             t1.contract_title,
#             t1.contract_description,
#             t1.framework_title,
#             t1.source,
#             t1.supplier_ids,
#             t2.id AS supplier_id,
#             t2.company_number,
#             t2.latest_employees
#         FROM dbo.Tussell_ContractAwards_API t1
#         CROSS APPLY OPENJSON(t1.supplier_ids) AS j
#         INNER JOIN dbo.Tussell_Suppliers_API t2 
#             ON CAST(j.value AS INT) = t2.id
#         WHERE (
#                 t1.framework_title LIKE 'RM1557.10%'
#                 OR t1.framework_title LIKE 'RM1557.11%'
#                 OR t1.framework_title LIKE 'RM1557.12%'
#                 OR t1.framework_title LIKE 'RM1557.13%'
#                 OR t1.framework_title LIKE 'RM1557.14%'
#             )
#     """
#     contracts = pd.read_sql(contracts_query, conn)
#     contracts = contracts.rename(columns={'company_number':'SupplierCompanyRegistrationNumber'})
#     print("Contracts parsed")

#     ## STEP 2: GET MI DATA
#     # connect to db using creds
#     conn_string = '{}://{}:{}@{}:{}/{}?driver={}'.format(
#         os.getenv("DB_TYPE"),
#         os.getenv("DB_USER"),
#         os.getenv("DB_PWD"),
#         os.getenv("DB_SERVER"),
#         os.getenv("DB_PORT"),
#         os.getenv("DB_NAME_MI"),
#         os.getenv("DB_DRIVER")
#     )
#     engine = create_engine(conn_string)
#     conn = engine.connect()

#     GCloud_MI = pd.DataFrame(columns = ['SupplierName','SupplierKey','CustomerName','FinancialYear','FinancialMonth','EvidencedSpend'])
#     for i in ['MI_RM155710', 'MI_RM155711', 'MI_RM155712', 'MI_RM155713', 'MI_RM155713L4', 'MI_RM155714', 'MI_RM155714L4']:
#         # find MI data for GCloud iteration
#         MI_query = f"""
#             SELECT SupplierName,SupplierKey,CustomerName,FinancialYear,FinancialMonth,EvidencedSpend FROM mi.{i}
#         """
#         MI_entries = pd.read_sql(MI_query, conn)
#         # add it to the aggregated df
#         GCloud_MI = pd.concat([GCloud_MI, MI_entries], axis=0)
#     print("MI parsed")

#     ## STEP 3: GET COMPANY REGISTRATION NUMBER - SUPPLIER KEY PAIRS
#     # connect to db using creds
#     conn_string = '{}://{}:{}@{}:{}/{}?driver={}'.format(
#         os.getenv("DB_TYPE"),
#         os.getenv("DB_USER"),
#         os.getenv("DB_PWD"),
#         os.getenv("DB_SERVER"),
#         os.getenv("DB_PORT"),
#         os.getenv("DB_NAME_REG"),
#         os.getenv("DB_DRIVER")
#     )
#     engine = create_engine(conn_string)
#     conn = engine.connect()
#     # find supplier Company Registration Number and CCS SupplierKey, to join Tussell to MI data
#     # also take supplier status
#     reg_number_supplier_key_query = """
#         SELECT SupplierKey,CompanyRegistrationNumber FROM sf.Attributes_sf_vw_Suppliers
#     """
#     reg_number_supplier_key = pd.read_sql(reg_number_supplier_key_query, conn)
#     reg_number_supplier_key = reg_number_supplier_key.rename(columns={'CompanyRegistrationNumber':'SupplierCompanyRegistrationNumber'})
#     print("Company Registration Numbers parsed")
#     return contracts, GCloud_MI, reg_number_supplier_key

# Use this block for checking the pipeline flow for live mode.
def download_live_data():
    """Download live data from databases."""
    # TEMPORARY: Mock for testing until DB credentials available
    print(" MOCK MODE: Using dummy data as live placeholder")
    print("TODO: Replace with real DB download once credentials received")
    return generate_dummy_data()

def main():
    # Get data based on mode
    if data_mode == 'dummy':
        contracts, GCloud_MI, reg_number_supplier_key = generate_dummy_data()
    elif data_mode == 'live':
        contracts, GCloud_MI, reg_number_supplier_key = download_live_data()
        # Stamp the folder to mark it as containing live data
        with open(stamp_file, 'w') as f:
            f.write("PROTECTED: This folder contains live production data.\n")
        print("Folder stamped as LIVE")
    else:
        raise ValueError(f"Invalid data_mode: {data_mode}. Must be 'dummy' or 'live'")

    # Save contracts DataFrame to CSV
    contracts.to_csv(os.path.join(output_dir, 'contracts.csv'), index=False)
    print(f"Saved contracts data to {os.path.join(output_dir, 'contracts.csv')}")

    # Save MI entries to CSV
    GCloud_MI.to_csv(os.path.join(output_dir, 'mi.csv'), index=False)
    print(f"Saved MI data to {os.path.join(output_dir, 'mi.csv')}")

    # Save reg numbers DataFrame to CSV
    reg_number_supplier_key.to_csv(os.path.join(output_dir, 'reg_key_pairs.csv'), index=False)
    print(f"Saved reg number data to {os.path.join(output_dir, 'reg_key_pairs.csv')}")

if __name__ == "__main__":
    main()