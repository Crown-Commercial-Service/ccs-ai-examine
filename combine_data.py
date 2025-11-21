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
    
    contracts_with_keys = contracts.merge(regno_keys, on="SupplierCompanyRegistrationNumber", how="inner")
    contracts_with_mi = contracts_with_keys.merge(mi, on="SupplierKey", how="left")
    return contracts_with_mi

if __name__ == "__main__":
    combined = combine_data(
        contracts_data="dummy_data/dummy_contracts.csv",
        mi_data="dummy_data/dummy_mi.csv",
        regno_key_pairs="dummy_data/dummy_reg_key_pairs.csv"
    )
    combined.to_csv("dummy_data/dummy_combined.csv", index=False)