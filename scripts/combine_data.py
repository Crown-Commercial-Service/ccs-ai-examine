import pandas as pd
import os
import sys
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI

# Add the parent directory to the path so that we can import from 'utils'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import match_string_with_langchain


def combine_data(contracts_data, mi_data, regno_key_pairs, combined_output, unmatched_output, model=None, chunk_size=1):
    """Combines contracts data with MI data
    Args:
        contracts_data: path to the contracts data CSV file
        mi_data: path to the MI data CSV file
        regno_key_pairs: path to the registration number - supplier key CSV file
        combined_output: path to write the combined data
        unmatched_output: path to write the unmatched data
        model: optional LangChain model for fuzzy matching
    """
    if os.path.exists(contracts_data):
        contracts = pd.read_csv(contracts_data, dtype={'SupplierCompanyRegistrationNumber': str})
    else:
        raise Exception(f"Contracts data file {contracts_data} does not exist")
    
    # We delay reading MI data here to do it in chunks later
    if not os.path.exists(mi_data):
        raise Exception(f"MI data file {mi_data} does not exist")

    if os.path.exists(regno_key_pairs):
        regno_keys = pd.read_csv(regno_key_pairs, dtype={'SupplierCompanyRegistrationNumber': str})
        regno_keys["SupplierKey"] = regno_keys["SupplierKey"].astype("Int64")
    else:
        raise Exception(f"Registration number - supplier key data file {regno_key_pairs} does not exist")
    
    # add supplier key onto contracts df
    contracts = contracts.merge(regno_keys, on="SupplierCompanyRegistrationNumber", how="inner")
    # add a unique reference value called "PairID" to each row of contracts and MI by concatenating the names of the buyer and supplier
    # lowercase the buyer names to avoid case differences throwing off the join
    contracts['PairID'] = contracts['SupplierKey'].astype(str) + '+' + contracts['buyer'].str.lower()
    
    buyer_names_from_contracts = contracts['buyer'].unique().tolist()
    
    # Cache for LLM matches to avoid re-querying across chunks
    ai_name_cache = {}

    # Initialize output files (write header once)
    first_chunk = True

    # Process MI in chunks
    for mi_chunk in pd.read_csv(mi_data, chunksize=chunk_size):
        mi = mi_chunk.copy()
        mi["SupplierKey"] = mi["SupplierKey"].astype("Int64")
        mi['PairID'] = mi['SupplierKey'].astype(str) + '+' + mi['CustomerName'].str.lower()

        # join MI onto contracts
        contracts_with_mi = contracts.merge(mi, on="PairID", how="left")
        matched_pair_ids = mi['PairID'].isin(contracts_with_mi['PairID'])
        
        unmatched_mi_all = mi[~matched_pair_ids]
        
        mi_buyer_names_to_ignore = unmatched_mi_all[unmatched_mi_all['CustomerName'].isin(buyer_names_from_contracts)]['CustomerName']
        unmatched_mi = unmatched_mi_all[~unmatched_mi_all['CustomerName'].isin(mi_buyer_names_to_ignore)].copy()

        if model and not unmatched_mi.empty:
            unique_unmatched_customers = unmatched_mi['CustomerName'].unique().tolist()
            
            # Check cache first, only ask LLM for new names
            customers_to_match = [c for c in unique_unmatched_customers if c not in ai_name_cache]
            
            count = 0
            for i in customers_to_match:
                name_match = match_string_with_langchain(i, buyer_names_from_contracts, model, './prompts/buyer_match_v2.txt')
                ai_name_cache[i] = name_match
                count += 1
                if count % 50 == 0:
                    print(f"Matched {count} / {len(customers_to_match)}")
            
            unmatched_mi['AIMatchedName'] = unmatched_mi['CustomerName'].map(ai_name_cache)
            # Ensure SupplierKey is treated as an integer string, to avoid mismatches due to float representations (e.g. '123.0' vs '123')
            unmatched_mi['PairID'] = unmatched_mi['SupplierKey'].astype('Int64').astype(str) + '+' + unmatched_mi['AIMatchedName'].str.lower()
            # join unmatched MI onto contracts
            contracts_with_mi_AI = contracts.merge(unmatched_mi, on="PairID", how="left")
            
            contracts_with_mi = pd.concat([contracts_with_mi, contracts_with_mi_AI])
            matched_pair_ids = mi['PairID'].isin(contracts_with_mi['PairID'])
            unmatched_mi = mi[~matched_pair_ids]

        # Append to files
        mode = 'w' if first_chunk else 'a'
        header = first_chunk
        
        contracts_with_mi.to_csv(combined_output, mode=mode, header=header, index=False)
        unmatched_mi.to_csv(unmatched_output, mode=mode, header=header, index=False)
        
        first_chunk = False

    return

if __name__ == "__main__":

    load_dotenv()

    model = AzureChatOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        openai_api_key=os.getenv("AZURE_OPENAI_KEY"),
        azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
        openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION")
    )

    # # run this block for live data
    # combine_data(
    #     contracts_data="data/contracts.csv",
    #     mi_data="data/mi.csv",
    #     regno_key_pairs="data/reg_number_supplier_key.csv",
    #     combined_output="data/combined.csv",
    #     unmatched_output="data/unmatched.csv",
    #     model=model
    # )

    # run this block for testing
    combine_data(
        contracts_data="dummy_data/dummy_contracts.csv",
        mi_data="dummy_data/dummy_mi.csv",
        regno_key_pairs="dummy_data/dummy_reg_key_pairs.csv",
        combined_output="dummy_data/dummy_combined.csv",
        unmatched_output="dummy_data/dummy_unmatched_mi.csv",
        model=model
    )

    # # run this block for debugging of isolated cases
    # combine_data(
    #     contracts_data="debugging/contracts.csv",
    #     mi_data="debugging/mi.csv",
    #     regno_key_pairs="debugging/reg_number_supplier_key.csv",
    #     combined_output="debugging/combined.csv",
    #     unmatched_output="debugging/unmatched_mi.csv",
    #     model=model
    # )