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
    os.getenv("DB_NAME_MI"),
    os.getenv("DB_DRIVER")
)
engine = create_engine(conn_string)
conn = engine.connect()
GCloud_MI = pd.DataFrame(columns = ['SupplierName','SupplierKey','CustomerName', 'CustomerGroup','FinancialYear','FinancialMonth','EvidencedSpend'])
for i in ['MI_RM155710', 'MI_RM155711', 'MI_RM155712', 'MI_RM155713', 'MI_RM155713L4', 'MI_RM155714', 'MI_RM155714L4']:
    # find MI data for GCloud iteration
    MI_query = f"""
        SELECT SupplierName,SupplierKey,CustomerName,CustomerGroup,FinancialYear,FinancialMonth,EvidencedSpend FROM mi.{i}
    """
    MI_entries = pd.read_sql(MI_query, conn)
    # add it to the aggregated df
    GCloud_MI = pd.concat([GCloud_MI, MI_entries], axis=0)
print("MI parsed")
combined = pd.read_csv("data/combined.csv", low_memory=False)
customer_name_group = GCloud_MI[['CustomerName', 'CustomerGroup']].copy()
customer_name_group = customer_name_group.drop_duplicates()
print(f"Before joining customer group, there are {len(combined)} entries in the combined dataframe")
combined = combined.merge(customer_name_group, on='CustomerName', how='left')
print(f"After joining customer group, there are {len(combined)} entries in the combined dataframe")
combined.to_csv("data/combined_with_CustomerGroup.csv", index=False)