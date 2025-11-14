import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()
# connect to db using creds
conn_string = '{}://{}:{}@{}:{}/{}?driver={}'.format(
    os.getenv("DB_TYPE"),
    os.getenv("DB_USER"),
    os.getenv("DB_PWD"),
    os.getenv("DB_SERVER"),
    os.getenv("DB_PORT"),
    os.getenv("DB_NAME"),
    os.getenv("DB_DRIVER")
)
engine = create_engine(conn_string)
conn = engine.connect()

# find buyer contract details
buyer_contracts_query = """
    SELECT TenderID,OCID,BuyerName,StartDate,EndDate,Amount,Title,Description FROM dbo.ContractNoticesAPI WHERE NoticeType='award'
"""
buyer_contracts = pd.read_sql(buyer_contracts_query, conn)
buyer_contracts['TenderID'] = buyer_contracts['TenderID'].astype(str)

# find suppliers for each contract
supplier_details_query = """
    SELECT TenderID,OCID,OrganisationName FROM dbo.ContractNoticesAPIParties WHERE Role='supplier'
"""
supplier_details = pd.read_sql(supplier_details_query, conn)
supplier_details = supplier_details.rename(columns={'OrganisationName': 'Supplier'})
supplier_details['TenderID'] = supplier_details['TenderID'].astype(str)

print(f"Before joining, there are {len(buyer_contracts)} rows of contracts and {len(supplier_details)} rows of suppliers")

# join buyer contracts onto suppliers
# using a left join because there could be multiple suppliers for a single tender, and we don't mind dropping contracts where we can't find the supplier 
combined = supplier_details.merge(buyer_contracts, on='TenderID', how='left')
print(f"After joining, there are {len(combined)} rows")