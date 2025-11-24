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
# find contract details for companies with a Company Registration Number (because these are the only ones that we can link into MI data)
contracts_query = """
    SELECT [Contracting Authority],Supplier,[Supplier Company Registration Number],[Contract Start Date],[Contract End Date],[Contract Duration (Months)],[Contract Title],[Contract Description],[Total Contract Value - Low (GBP)],[Total Contract Value - High (GBP)] FROM dbo.Tussell_ContractNotices WHERE [Supplier Company Registration Number] IS NOT NULL
"""
contracts = pd.read_sql(contracts_query, conn)
contracts = contracts.rename(columns={'Supplier Company Registration Number':'SupplierCompanyRegistrationNumber'})
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
# find MI data for GCloud 14
GCloud14_MI_query = """
    SELECT SupplierName,SupplierKey,CustomerName,FinancialYear,FinancialMonth,EvidencedSpend FROM mi.MI_RM155714
"""
GCloud14_MI = pd.read_sql(GCloud14_MI_query, conn)
print("MI parsed")

# Save GCloud14_MI DataFrame to CSV
GCloud14_MI.to_csv(os.path.join(output_dir, 'mi.csv'), index=False)
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
reg_number_supplier_key_query = """
    SELECT SupplierKey,CompanyRegistrationNumber FROM sf.Attributes_sf_vw_Suppliers
"""
reg_number_supplier_key = pd.read_sql(reg_number_supplier_key_query, conn)
reg_number_supplier_key = reg_number_supplier_key.rename(columns={'CompanyRegistrationNumber':'SupplierCompanyRegistrationNumber'})
print("Company Registration Numbers parsed")

# Save reg numbers DataFrame to CSV
reg_number_supplier_key.to_csv(os.path.join(output_dir, 'reg_number_supplier_key.csv'), index=False)
print(f"Saved reg number data to {os.path.join(output_dir, 'reg_number_supplier_key.csv')}")