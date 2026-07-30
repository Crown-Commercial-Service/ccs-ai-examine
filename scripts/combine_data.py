import pandas as pd
import os
import argparse
from dotenv import load_dotenv
from utils import match_strings_via_api_concurrent


def _usable_name_mask(series):
    """Return rows containing real, nonblank names (not NaN/None)."""
    return series.notna() & series.astype(str).str.strip().ne("")


def _report_missing_customer_names(mi, mask):
    """Print enough source-row context to find invalid API inputs in the CSV."""
    invalid = mi.loc[~mask]
    if invalid.empty:
        return

    # DataFrame index 0 is CSV line 2 because line 1 contains the headings.
    examples = []
    for index, row in invalid.head(10).iterrows():
        examples.append(
            f"csv_line={index + 2}, dataframe_index={index}, "
            f"CustomerName={row.get('CustomerName')!r}, "
            f"SupplierKey={row.get('SupplierKey')!r}"
        )
    print(
        f"WARNING: {len(invalid)} MI row(s) have a missing or blank CustomerName. "
        "They will not be sent to the name-match API and will remain unmatched. "
        f"Examples: {'; '.join(examples)}",
        flush=True,
    )


def combine_data(contracts_data, mi_data, regno_key_pairs):
    """Combines contracts data with MI data
    Args:
        contracts_data: path to the contracts data CSV file
        mi_data: path to the MI data CSV file
        regno_key_pairs: path to the registration number - supplier key CSV file
    """
    if os.path.exists(contracts_data):
        contracts = pd.read_csv(
            contracts_data, dtype={"SupplierCompanyRegistrationNumber": str}
        )
    else:
        raise Exception(f"Contracts data file {contracts_data} does not exist")
    if os.path.exists(mi_data):
        mi = pd.read_csv(mi_data)
        mi["SupplierKey"] = mi["SupplierKey"].astype("Int64")
    else:
        raise Exception(f"MI data file {mi_data} does not exist")
    if os.path.exists(regno_key_pairs):
        regno_keys = pd.read_csv(
            regno_key_pairs, dtype={"SupplierCompanyRegistrationNumber": str}
        )
        regno_keys["SupplierKey"] = regno_keys["SupplierKey"].astype("Int64")
    else:
        raise Exception(
            f"Registration number - supplier key data file {regno_key_pairs} does not exist"
        )

    # Report malformed source data before string operations turn NaN into an opaque API error.
    valid_mi_customer = _usable_name_mask(mi["CustomerName"])
    _report_missing_customer_names(mi, valid_mi_customer)

    # add supplier key onto contracts df
    contracts = contracts.merge(
        regno_keys, on="SupplierCompanyRegistrationNumber", how="inner"
    )
    # add a unique reference value called "PairID" to each row of contracts and MI by concatenating the names of the buyer and supplier
    # lowercase the buyer names to avoid case differences throwing off the join
    contracts["PairID"] = (
        contracts["SupplierKey"].astype(str) + "+" + contracts["buyer"].str.lower()
    )
    mi["PairID"] = mi["SupplierKey"].astype(str) + "+" + mi["CustomerName"].str.lower()
    # join MI onto contracts
    contracts_with_mi = contracts.merge(mi, on="PairID", how="left")
    matched_pair_ids = mi["PairID"].isin(contracts_with_mi["PairID"])
    # find the unmatched MI, which may be because
    # Situation 1. the buyer name in the MI matches to one in the contract data, and there is simply no contract with a supplier
    # Situation 2. the buyer name in the MI doesn't match to one in the contract data, and we need an LLM to find a match
    # we can safely ignore Situation 1: if the name matches, we would already have caught it in the initial join, and all the LLM will return is its input
    unmatched_mi_all = mi[~matched_pair_ids]
    # Do not send missing CustomerName values to the API. They cannot be matched and
    # are retained in the final unmatched output.
    unmatched_mi_all_valid = unmatched_mi_all.loc[
        _usable_name_mask(unmatched_mi_all["CustomerName"])
    ]
    # ignore Situation 1
    buyer_names_from_contracts = (
        contracts.loc[_usable_name_mask(contracts["buyer"]), "buyer"]
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )
    mi_buyer_names_to_ignore = unmatched_mi_all_valid[
        unmatched_mi_all_valid["CustomerName"].isin(buyer_names_from_contracts)
    ]["CustomerName"]
    # focus on Situation 2
    unmatched_mi = unmatched_mi_all_valid[
        ~unmatched_mi_all_valid["CustomerName"].isin(mi_buyer_names_to_ignore)
    ].copy()

    # Matching is handled by the external API.
    # Set MATCH_STRING_API_URL to your external `GET /match` endpoint.
    if not unmatched_mi.empty:
        unique_unmatched_customers = unmatched_mi["CustomerName"].unique().tolist()
        print(f"Total unique unmatched customers = {len(unique_unmatched_customers)}")
        name_map = match_strings_via_api_concurrent(
            input_strings=unique_unmatched_customers,
            list_of_strings=buyer_names_from_contracts,
            prompt_path="./prompts/buyer_match_v2.txt",
            api_url=os.getenv("NAME_MATCH_API_ENDPOINT"),
            max_workers=int(os.getenv("MATCH_STRING_MAX_WORKERS", "8")),
            show_progress=True,
            progress_desc="Matching buyers via API",
        )
        unmatched_mi["AIMatchedName"] = unmatched_mi["CustomerName"].map(name_map)
        # Ensure SupplierKey is treated as an integer string, to avoid mismatches due to float representations (e.g. '123.0' vs '123')
        unmatched_mi["PairID"] = (
            unmatched_mi["SupplierKey"].astype("Int64").astype(str)
            + "+"
            + unmatched_mi["AIMatchedName"].str.lower()
        )
        # join unmatched MI onto contracts
        contracts_with_mi_AI = contracts.merge(unmatched_mi, on="PairID", how="left")

        contracts_with_mi = pd.concat([contracts_with_mi, contracts_with_mi_AI])

    # Recalculate against the full MI table so skipped null names remain unmatched.
    matched_pair_ids = mi["PairID"].isin(contracts_with_mi["PairID"])
    unmatched_mi = mi[~matched_pair_ids]

    return (contracts_with_mi, unmatched_mi)


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--indir", required=True)
    parser.add_argument("--outdir", required=True)
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    combined, unmatched = combine_data(
        contracts_data=os.path.join(args.indir, "contracts.csv"),
        mi_data=os.path.join(args.indir, "mi.csv"),
        regno_key_pairs=os.path.join(args.indir, "reg_number_supplier_key.csv"),
    )
    combined.to_csv(os.path.join(args.outdir, "combined.csv"), index=False)
    unmatched.to_csv(os.path.join(args.outdir, "unmatched.csv"), index=False)
