import pandas as pd
import os

def combine_data(contracts_data, mi_data, regno_key_pairs):
    """Combines contracts data with MI data
    Args:
        contracts_data: path to the contracts data CSV file
        mi_data: path to the MI data CSV file
        regno_key_pairs: path to the registration number - supplier key CSV file
    """
    if os.path.exists(contracts_data):
        contracts = pd.read_csv(contracts_data)
    else:
        raise Exception(f"Contracts data file {contracts_data} does not exist")
    if os.path.exists(mi_data):
        mi = pd.read_csv(mi_data)
    else:
        raise Exception(f"MI data file {mi_data} does not exist")
    if os.path.exists(regno_key_pairs):
        regno_keys = pd.read_csv(regno_key_pairs)
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
    unmatched_pair_ids = mi['PairID'].isin(contracts_with_mi['PairID'])
    unmatched_mi = mi[~unmatched_pair_ids]
    return (contracts_with_mi, unmatched_mi)

if __name__ == "__main__":

    # # for testing purposes only
    # combined, unmatched = combine_data(
    #     contracts_data="dummy_data/dummy_contracts.csv",
    #     mi_data="dummy_data/dummy_mi.csv",
    #     regno_key_pairs="dummy_data/dummy_reg_key_pairs.csv"
    # )
    # combined.to_csv("dummy_data/dummy_combined.csv", index=False)
    # unmatched.to_csv("dummy_data/dummy_unmatched_mi.csv", index=False)

    combined, unmatched = combine_data(
        contracts_data="data/contracts.csv",
        mi_data="data/mi.csv",
        regno_key_pairs="data/reg_number_supplier_key.csv"
    )
    combined.to_csv("data/combined.csv", index=False)
    unmatched.to_csv("data/unmatched.csv", index=False)