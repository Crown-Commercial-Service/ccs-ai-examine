"""
Pipeline script for data summarization stage.
Uses paths from params.yaml to read/write files.
"""
import yaml
import pandas as pd

def main():
    # Load params
    with open('params.yaml', 'r') as f:
        params = yaml.safe_load(f)
    
    data_mode = params['data_mode']
    paths = params['paths'][data_mode]
    
    print(f"Running summarise_data in {data_mode.upper()} mode...")
    
    # Read in data
    contracts = pd.read_csv(paths['contracts'], low_memory=False)
    matched = pd.read_csv(paths['combined'], low_memory=False)
    matched['contract_start'] = pd.to_datetime(matched['contract_start'])
    matched['contract_end'] = pd.to_datetime(matched['contract_end'])
    matched = matched.rename(columns={
        'buyer': 'Contracting Authority',
        'suppliers': 'Supplier',
        'award_value': 'Award Value',
        'contract_start': 'Contract Start Date',
        'contract_end': 'Contract End Date',
        'contract_months': 'Contract Duration (Months)',
    })
    unmatched = pd.read_csv(paths['unmatched'], low_memory=False)
    
    # Find most recent contract per buyer-supplier pair
    matched['MostRecentStartDate'] = matched.groupby(['Contracting Authority', 'Supplier'])['Contract Start Date'].transform('max')
    recent_contracts_only = matched[matched['Contract Start Date'] == matched['MostRecentStartDate']]
    
    # Aggregate spend
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
        'latest_employees': 'first'
    }).reset_index()
    
    # Calculate months run
    now = pd.Timestamp.now().normalize()
    reported_spend_per_pair['Total Months Run So Far'] = (
        (now.year - reported_spend_per_pair['Contract Start Date'].dt.year) * 12 + 
        (now.month - reported_spend_per_pair['Contract Start Date'].dt.month)
    )
    
    # Find expired contracts
    reported_spend_per_pair['Expired'] = reported_spend_per_pair['Contract End Date'] < now
    expired_contracts = reported_spend_per_pair[reported_spend_per_pair['Expired']==True].copy()
    
    # Summary statistics
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
        "Summary Statistic": ["Total Contracts", "Total Contracts with Supplier Key", "Unmatched MI Entries", 
                              "Unique Unmatched Suppliers", "Unique Unmatched Buyers",
                              "Total Contracts with Spend", "Total Contracts No Spend and Expired", 
                              "Total Contracts No Spend and Running >3 Months"],
        "Value": [total_contracts, total_contracts_with_key, unmatched_mi_entries, unique_unmatched_suppliers, 
                  unique_unmatched_buyers, total_contracts_with_spend, no_spend_expired, no_spend_3month_run]
    }
    summary_stats_df = pd.DataFrame(summary_stats)
    
    # Save outputs
    summary_stats_df.to_csv(paths['summary_stats'], index=False)
    reported_spend_per_pair.to_csv(paths['line_level'], index=False)
    
    print("\nSummary Statistics:")
    print(summary_stats_df)
    print(f"\nData summarization complete. Saved to:")
    print(f"  - {paths['summary_stats']}")
    print(f"  - {paths['line_level']}")

if __name__ == "__main__":
    main()