import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

# first take CustomerGroup from MI data, as this deals with name mismatches
# note: this only brings in CustomerGroup for customers that a supplier has reported MI for
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
customer_name_group_from_mi = pd.DataFrame(columns = ['CustomerName', 'CustomerGroup'])
for i in ['MI_RM155710', 'MI_RM155711', 'MI_RM155712', 'MI_RM155713', 'MI_RM155713L4', 'MI_RM155714', 'MI_RM155714L4']:
    # find MI data for GCloud iteration
    MI_query = f"""
        SELECT CustomerName,CustomerGroup FROM mi.{i}
    """
    MI_entries = pd.read_sql(MI_query, conn)
    # add it to the aggregated df
    customer_name_group_from_mi = pd.concat([customer_name_group_from_mi, MI_entries], axis=0)
print(f"{len(customer_name_group_from_mi)} entries from MI parsed")
print(customer_name_group_from_mi.head())

# then take CustomerGroup from Salesforce data, as this will give matches even if no MI has been reported
# note: this doesn't deal with name mismatches
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
customer_name_group_from_sf_query = "SELECT CustomerName,[Group] FROM sf.Attributes_sf_vw_Customers"
customer_name_group_from_sf = pd.read_sql(customer_name_group_from_sf_query, conn)
# change to match with MI headers
customer_name_group_from_sf = customer_name_group_from_sf.rename(columns={'Group': 'CustomerGroup'})
print(f"{len(customer_name_group_from_sf)} entries from Salesforce parsed")
print(customer_name_group_from_sf.head())

# combine MI and Salesforce name groups together, find uniques, and convert to dict
customer_name_group_df = pd.concat([customer_name_group_from_mi, customer_name_group_from_sf], axis=0)
print(f"MI and Salesforce combined in one df with {len(customer_name_group_df)} entries")
customer_name_group_df = customer_name_group_df.drop_duplicates()
customer_name_group_dict = dict(zip(customer_name_group_df['CustomerName'], customer_name_group_df['CustomerGroup']))

# read in combined data, and map 
combined = pd.read_csv("data/combined.csv", low_memory=False)
print(f"Before adding customer group, there are {len(combined)} entries in the combined dataframe")
# remove anything after opening a bracket, because these elements throw off matching
combined['CustomerGroup'] = combined['buyer'].str.split("(").str[0].str.strip()
combined['CustomerGroup'] = combined['CustomerGroup'].str.replace('&', 'and')
combined['CustomerGroup'] = combined['CustomerGroup'].map(customer_name_group_dict)
print(f"After joining customer group, there are {len(combined)} entries in the combined dataframe")
combined.to_csv("data/combined_with_CustomerGroup.csv", index=False)