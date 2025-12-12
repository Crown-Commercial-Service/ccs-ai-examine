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
# find GCloud 10-14 contract details for companies with a Company Registration Number (because these are the only ones that we can link into MI data)
# also add lot number
contracts_query = """
    SELECT [Contracting Authority],Supplier,[Supplier Company Registration Number],[Date Published],[Contract Start Date],[Contract End Date],[Contract Duration (Months)],[Contract Title],[Contract Description],[Total Contract Value - Low (GBP)],[Total Contract Value - High (GBP)],[Framework Contract] FROM dbo.Tussell_ContractNotices
    WHERE (
        [Framework Contract] LIKE 'RM1557.10%'
        OR [Framework Contract] LIKE 'RM1557.11%'
        OR [Framework Contract] LIKE 'RM1557.12%'
        OR [Framework Contract] LIKE 'RM1557.13%'
        OR [Framework Contract] LIKE 'RM1557.14%'
    )
    AND [Supplier Company Registration Number] IS NOT NULL
"""
contracts = pd.read_sql(contracts_query, conn)
contracts = contracts.rename(columns={'Supplier Company Registration Number':'SupplierCompanyRegistrationNumber'})
print("Contracts parsed")

# Create output directory if it doesn't exist
output_dir = 'data'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# before writing to file, we need to deal with an issue with the supplier company registration numbers
# these are always 8 characters long, and most are numeric, but some can have a two-letter prefix followed by 6 numbers
# some start with zeros, but the contracts df doesn't always keep these leading zeros
# this will break the join with any table that does retain them, like the regno_keys table
# we can't just convert everything to integers before joining, because some tiny fraction of suppliers may have the two-letter prefix
# therefore we need to find reg. nos. in contracts df which have <8 characters in their supplier company registration numbers, and add the zero(es) back in
contracts['SupplierCompanyRegistrationNumber'] = [i.zfill(8) for i in contracts['SupplierCompanyRegistrationNumber']]

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

GCloud_MI = pd.DataFrame(columns = ['SupplierName','SupplierKey','CustomerName','FinancialYear','FinancialMonth','EvidencedSpend'])
for i in ['MI_RM155710', 'MI_RM155711', 'MI_RM155712', 'MI_RM155713', 'MI_RM155713L4', 'MI_RM155714', 'MI_RM155714L4']:
    # find MI data for GCloud iteration
    MI_query = f"""
        SELECT SupplierName,SupplierKey,CustomerName,FinancialYear,FinancialMonth,EvidencedSpend FROM mi.{i}
    """
    MI_entries = pd.read_sql(MI_query, conn)
    # add it to the aggregated df
    GCloud_MI = pd.concat([GCloud_MI, MI_entries], axis=0)
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
