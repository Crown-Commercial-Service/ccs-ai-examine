import pandas as pd
import os
import sys
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI

# Add the parent directory to the path so that we can import from 'utils'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import match_string_with_langchain


def combine_data(contracts_data, mi_data, regno_key_pairs, model=None):
    """Combines contracts data with MI data
    Args:
        contracts_data: path to the contracts data CSV file
        mi_data: path to the MI data CSV file
        regno_key_pairs: path to the registration number - supplier key CSV file
        model: optional LangChain model for fuzzy matching
    """
    if os.path.exists(contracts_data):
        contracts = pd.read_csv(contracts_data)
    else:
        raise Exception(f"Contracts data file {contracts_data} does not exist")
    if os.path.exists(mi_data):
        mi = pd.read_csv(mi_data)
        mi["SupplierKey"] = mi["SupplierKey"].astype("Int64")
    else:
        raise Exception(f"MI data file {mi_data} does not exist")
    if os.path.exists(regno_key_pairs):
        regno_keys = pd.read_csv(regno_key_pairs)
        regno_keys["SupplierKey"] = regno_keys["SupplierKey"].astype("Int64")
    else:
        raise Exception(f"Registration number - supplier key data file {regno_key_pairs} does not exist")
    
    # add supplier key onto contracts df
    contracts = contracts.merge(regno_keys, on="SupplierCompanyRegistrationNumber", how="inner")
    # add a unique reference value called "PairID" to each row of contracts and MI by concatenating the names of the buyer and supplier
    # lowercase the buyer names to avoid case differences throwing off the join
    contracts['PairID'] = contracts['SupplierKey'].astype(str) + '+' + contracts['Contracting Authority'].str.lower()
    mi['PairID'] = mi['SupplierKey'].astype(str) + '+' + mi['CustomerName'].str.lower()
    # join MI onto contracts
    contracts_with_mi = contracts.merge(mi, on="PairID", how="left")
    matched_pair_ids = mi['PairID'].isin(contracts_with_mi['PairID'])
    unmatched_mi = mi[~matched_pair_ids]

    if model and not unmatched_mi.empty:
        unmatched_mi = unmatched_mi.copy()
        authority_names = contracts['Contracting Authority'].unique().tolist()
        unique_unmatched_customers = unmatched_mi['CustomerName'].unique()
        name_map = {name: match_string_with_langchain(name, authority_names, model) for name in unique_unmatched_customers}
        unmatched_mi['AIMatchedName'] = unmatched_mi['CustomerName'].map(name_map)
        unmatched_mi['PairID'] = unmatched_mi['SupplierKey'].astype(str) + '+' + unmatched_mi['AIMatchedName'].str.lower()
        # join unmatched MI onto contracts that weren't matched the first time
        matched_contracts_pair_ids = contracts['PairID'].isin(contracts_with_mi['PairID'])
        unmatched_contracts = contracts[~matched_contracts_pair_ids]
        contracts_with_mi_AI = unmatched_contracts.merge(unmatched_mi, on="PairID", how="left")
        contracts_with_mi = pd.concat([contracts_with_mi, contracts_with_mi_AI])
        matched_pair_ids = mi['PairID'].isin(contracts_with_mi['PairID'])
        unmatched_mi = mi[~matched_pair_ids]

    return (contracts_with_mi, unmatched_mi)

if __name__ == "__main__":

    load_dotenv()

    model = AzureChatOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        openai_api_key=os.getenv("AZURE_OPENAI_KEY"),
        azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
        openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION")
    )

    # # run this block for live data
    # combined, unmatched = combine_data(
    #     contracts_data="data/contracts.csv",
    #     mi_data="data/mi.csv",
    #     regno_key_pairs="data/reg_number_supplier_key.csv",
    #     model=model
    # )
    # combined.to_csv("data/combined.csv", index=False)
    # unmatched.to_csv("data/unmatched.csv", index=False)

    # run this block for testing
    combined, unmatched = combine_data(
        contracts_data="dummy_data/dummy_contracts.csv",
        mi_data="dummy_data/dummy_mi.csv",
        regno_key_pairs="dummy_data/dummy_reg_key_pairs.csv",
        model=model
    )
    combined.to_csv("dummy_data/dummy_combined.csv", index=False)
    unmatched.to_csv("dummy_data/dummy_unmatched_mi.csv", index=False)