"""
Combines contracts data with MI data.
"""
import pandas as pd
import os
import sys
import yaml
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI

# Add parent to path for utils import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import batch_match_string_with_langchain

def combine_data(contracts_data, mi_data, regno_key_pairs, model=None):
    """Combines contracts data with MI data
    Args:
        contracts_data: path to the contracts data CSV file
        mi_data: path to the MI data CSV file
        regno_key_pairs: path to the registration number - supplier key CSV file
        model: optional LangChain model for fuzzy matching
    """
    if os.path.exists(contracts_data):
        contracts = pd.read_csv(contracts_data, dtype={'SupplierCompanyRegistrationNumber': str})
    else:
        raise Exception(f"Contracts data file {contracts_data} does not exist")
    if os.path.exists(mi_data):
        mi = pd.read_csv(mi_data)
        mi["SupplierKey"] = mi["SupplierKey"].astype("Int64")
    else:
        raise Exception(f"MI data file {mi_data} does not exist")
    if os.path.exists(regno_key_pairs):
        regno_keys = pd.read_csv(regno_key_pairs, dtype={'SupplierCompanyRegistrationNumber': str})
        regno_keys["SupplierKey"] = regno_keys["SupplierKey"].astype("Int64")
    else:
        raise Exception(f"Registration number - supplier key data file {regno_key_pairs} does not exist")
    
    # Add supplier key onto contracts
    contracts = contracts.merge(regno_keys, on="SupplierCompanyRegistrationNumber", how="inner")
    
    # Create PairID
    contracts['PairID'] = contracts['SupplierKey'].astype(str) + '+' + contracts['buyer'].str.lower()
    mi['PairID'] = mi['SupplierKey'].astype(str) + '+' + mi['CustomerName'].str.lower()
    
    # Join MI onto contracts
    contracts_with_mi = contracts.merge(mi, on="PairID", how="left")
    matched_pair_ids = mi['PairID'].isin(contracts_with_mi['PairID'])
    
    # Find unmatched MI
    unmatched_mi_all = mi[~matched_pair_ids]
    buyer_names_from_contracts = contracts['buyer'].unique().tolist()
    mi_buyer_names_to_ignore = unmatched_mi_all[unmatched_mi_all['CustomerName'].isin(buyer_names_from_contracts)]['CustomerName']
    unmatched_mi = unmatched_mi_all[~unmatched_mi_all['CustomerName'].isin(mi_buyer_names_to_ignore)].copy()

    # LLM matching for unmatched
    if model and not unmatched_mi.empty:
        unique_unmatched_customers = unmatched_mi['CustomerName'].unique().tolist()
        matched_names = batch_match_string_with_langchain(unique_unmatched_customers, buyer_names_from_contracts, model)
        name_map = dict(zip(unique_unmatched_customers, matched_names))
        unmatched_mi['AIMatchedName'] = unmatched_mi['CustomerName'].map(name_map)
        unmatched_mi['PairID'] = unmatched_mi['SupplierKey'].astype('Int64').astype(str) + '+' + unmatched_mi['AIMatchedName'].str.lower()
        contracts_with_mi_AI = contracts.merge(unmatched_mi, on="PairID", how="left")
        
        contracts_with_mi = pd.concat([contracts_with_mi, contracts_with_mi_AI])
        matched_pair_ids = mi['PairID'].isin(contracts_with_mi['PairID'])
        unmatched_mi = mi[~matched_pair_ids]

    return (contracts_with_mi, unmatched_mi)


def main():
    """Main function for pipeline execution."""
    with open('params.yaml', 'r') as f:
        params = yaml.safe_load(f)
    
    data_mode = params['data_mode']
    paths = params['paths'][data_mode]
    
    print(f"Running combine_data in {data_mode.upper()} mode...")
    
    load_dotenv()
    
    model = AzureChatOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        openai_api_key=os.getenv("AZURE_OPENAI_KEY"),
        azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
        openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION")
    )
    
    combined, unmatched = combine_data(
        contracts_data=paths['contracts'],
        mi_data=paths['mi'],
        regno_key_pairs=paths['regno'],
        model=model
    )
    
    combined.to_csv(paths['combined'], index=False)
    unmatched.to_csv(paths['unmatched'], index=False)
    
    print(f"Data combination complete. Saved to:")
    print(f"  - {paths['combined']}")
    print(f"  - {paths['unmatched']}")


if __name__ == "__main__":
    main()