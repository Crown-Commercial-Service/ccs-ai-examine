import pandas as pd

# read in data and make sure that column types are correct
matched = pd.read_csv("data/combined.csv", low_memory=False)
unmatched = pd.read_csv("data/unmatched.csv", low_memory=False)
reported_spend_per_contract = matched.groupby('PairID').agg({
    'Contracting Authority': 'first',
    'Supplier': 'first',
    'Contract Start Date': 'first',
    'Contract End Date': 'first',
    'Contract Duration (Months)': 'first',
    'EvidencedSpend': 'sum'
}).reset_index(drop=True)
reported_spend_per_contract['Contract Start Date'] = pd.to_datetime(reported_spend_per_contract['Contract Start Date'])
reported_spend_per_contract['Contract End Date'] = pd.to_datetime(reported_spend_per_contract['Contract End Date'])
# add a column of the number of months between each start date and the present day
now = pd.Timestamp.now().normalize()
reported_spend_per_contract['Total Months Run So Far'] = (
    (now.year - reported_spend_per_contract['Contract Start Date'].dt.year) * 12 + 
    (now.month - reported_spend_per_contract['Contract Start Date'].dt.month)
)
# find expired contracts
reported_spend_per_contract['Expired'] = reported_spend_per_contract['Contract End Date'] < now
expired_contracts = reported_spend_per_contract[reported_spend_per_contract['Expired']==True].copy()

# summary stats
print(f"There are {len(reported_spend_per_contract)} contracts in total")
print(f"There are {len(unmatched)} unmatched MI entries in total, featuring {len(unmatched['SupplierName'].unique())} suppliers and {len(unmatched['CustomerName'].unique())} buyers")
print(f"There are {len(reported_spend_per_contract[reported_spend_per_contract['EvidencedSpend']>0.0])} contracts with spend reported")

# flags
red_filter = expired_contracts['EvidencedSpend']==0.0
print(f"There are {len(expired_contracts[red_filter])} out of {len(expired_contracts)} expired contracts that have no reported spend")
amber_filter = (reported_spend_per_contract['Expired']==False) & (reported_spend_per_contract['Total Months Run So Far']>3) & (reported_spend_per_contract['EvidencedSpend']==0.0)
print(f"There are {len(reported_spend_per_contract[amber_filter])} out of {len(reported_spend_per_contract)} current contracts that have been running for >3 months and have no reported spend")