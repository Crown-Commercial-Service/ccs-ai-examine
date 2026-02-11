import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

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
contracts = contracts.rename(columns={'company_number':'SupplierCompanyRegistrationNumber'})
print("Contracts parsed")

# Create output directory if it doesn't exist
output_dir = 'data'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Save contracts DataFrame to CSV
contracts.to_csv(os.path.join(output_dir, 'contracts.csv'), index=False)
print(f"Saved contracts data to {os.path.join(output_dir, 'contracts.csv')}")

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

MI_query = """
        SELECT SupplierName,SupplierKey,CustomerName,[Group],FinancialYear,FinancialMonth,EvidencedSpend FROM dbo.AggregatedSpendReporting
        WHERE FrameworkName LIKE 'G-Cloud 1%'
    """
GCloud_MI = pd.read_sql(MI_query, conn)
GCloud_MI = GCloud_MI.rename(columns={'Group':'CustomerGroup'})
print("MI parsed")

# Save MI entries to CSV
GCloud_MI.to_csv(os.path.join(output_dir, 'mi.csv'), index=False)
print(f"Saved MI data to {os.path.join(output_dir, 'mi.csv')}")

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
reg_number_supplier_key = reg_number_supplier_key.rename(columns={'CompanyRegistrationNumber':'SupplierCompanyRegistrationNumber'})
print("Company Registration Numbers parsed")
# Save reg numbers DataFrame to CSV
reg_number_supplier_key.to_csv(os.path.join(output_dir, 'reg_number_supplier_key.csv'), index=False)
print(f"Saved reg number data to {os.path.join(output_dir, 'reg_number_supplier_key.csv')}")
