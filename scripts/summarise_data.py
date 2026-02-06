import pandas as pd
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--indir", required=True)
parser.add_argument("--outdir", required=True)
args = parser.parse_args()

os.makedirs(args.outdir, exist_ok=True)

# read in data and make sure that column types are correct
contracts = pd.read_csv(os.path.join(args.indir, "contracts.csv"), low_memory=False)
matched = pd.read_csv(
    os.path.join(args.indir, "combined.csv"),
    low_memory=False
)
matched['contract_start'] = pd.to_datetime(matched['contract_start'])
matched['contract_end'] = pd.to_datetime(matched['contract_end'])
matched = matched.rename(columns={
    'buyer': 'Contracting Authority',
    'suppliers': 'Supplier',
    'award_value': 'Award Value',
    'contract_start': 'Contract Start Date',
    'contract_end': 'Contract End Date',
    'contract_months': 'Contract Duration (Months)',
    'CustomerGroup': 'Customer Group'
})
unmatched = pd.read_csv(os.path.join(args.indir, "unmatched.csv"), low_memory=False)
# For each buyer-supplier pair, find the most recent contract (or contracts, if they share the same start date)
matched['MostRecentStartDate'] = matched.groupby(['Contracting Authority', 'Supplier'])['Contract Start Date'].transform('max')
recent_contracts_only = matched[matched['Contract Start Date'] == matched['MostRecentStartDate']]

# For each buyer-supplier pair, aggregate the spend from their most recent contract(s)
reported_spend_per_pair = recent_contracts_only.groupby(['Contracting Authority', 'Supplier']).agg({
    'awarded': 'first',
    'Award Value': 'first',
    'EvidencedSpend': 'sum',
    'Contract Start Date': 'first',
    'Contract End Date': 'first',
    'Contract Duration (Months)': 'first',
    'contract_title': 'first',
    'contract_description': 'first',
    'framework_title': 'first',
    'source': 'first',
    'latest_employees': 'first',
    'Customer Group': 'first'
}).reset_index()
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
summary_stats_df.to_csv(os.path.join(args.outdir, "summary_stats.csv"), index=False)

# line-level data output
reported_spend_per_pair.to_csv(os.path.join(args.outdir, "line_level.csv"), index=False)