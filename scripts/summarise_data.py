import pandas as pd

# read in data and make sure that column types are correct
contracts = pd.read_csv("data/contracts.csv", low_memory=False)
matched = pd.read_csv("data/combined.csv", low_memory=False)
unmatched = pd.read_csv("data/unmatched.csv", low_memory=False)
# where a buyer-supplier pair has >1 contract, take the most recent contract
reported_spend_per_pair = matched.sort_values('Contract Start Date', ascending=False).groupby('PairID').agg({
    'Contracting Authority': 'first',
    'Supplier': 'first',
    'Total Contract Value - High (GBP)': 'sum',
    'Contract Start Date': 'first',
    'Contract End Date': 'first',
    'Contract Duration (Months)': 'first',
    'EvidencedSpend': 'sum'
}).reset_index(drop=True)
reported_spend_per_pair['Contract Start Date'] = pd.to_datetime(reported_spend_per_pair['Contract Start Date'])
reported_spend_per_pair['Contract End Date'] = pd.to_datetime(reported_spend_per_pair['Contract End Date'])
# add a column of the number of months between each start date and the present day
now = pd.Timestamp.now().normalize()
reported_spend_per_pair['Total Months Run So Far'] = (
    (now.year - reported_spend_per_pair['Contract Start Date'].dt.year) * 12 + 
    (now.month - reported_spend_per_pair['Contract Start Date'].dt.month)
)
# find expired contracts
reported_spend_per_pair['Expired'] = reported_spend_per_pair['Contract End Date'] < now
expired_contracts = reported_spend_per_pair[reported_spend_per_pair['Expired']==True].copy()

# summary stats
total_contracts = len(contracts)
total_contracts_with_key = len(reported_spend_per_pair)
unmatched_mi_entries = len(unmatched)
unique_unmatched_suppliers = len(unmatched['SupplierName'].unique())
unique_unmatched_buyers = len(unmatched['CustomerName'].unique())
total_contracts_with_spend = len(reported_spend_per_pair[reported_spend_per_pair['EvidencedSpend']>0.0])
red_filter = expired_contracts['EvidencedSpend']==0.0
no_spend_expired = len(expired_contracts[red_filter])
amber_filter = (reported_spend_per_pair['Expired']==False) & (reported_spend_per_pair['Total Months Run So Far']>3) & (reported_spend_per_pair['EvidencedSpend']==0.0)
no_spend_3month_run = len(reported_spend_per_pair[amber_filter])
summary_stats = {
    "Summary Statistic": ["Total Contracts", "Total Contracts with Supplier Key", "Unmatched MI Entries", "Unique Unmatched Suppliers", "Unique Unmatched Buyers",
                          "Total Contracts with Spend", "Total Contracts No Spend and Expired", "Total Contracts No Spend and Running >3 Months"],
    "Value": [total_contracts, total_contracts_with_key, unmatched_mi_entries, unique_unmatched_suppliers, unique_unmatched_buyers,
              total_contracts_with_spend, no_spend_expired, no_spend_3month_run]
}
summary_stats_df = pd.DataFrame(summary_stats)
print(summary_stats_df)
summary_stats_df.to_csv("data/summary_stats.csv", index=False)

# line-level data output
reported_spend_per_pair.to_csv("data/line_level.csv", index=False)