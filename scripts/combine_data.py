import argparse
import os
from typing import Dict, Optional, Set, Tuple

import pandas as pd
from dotenv import load_dotenv

from utils import match_strings_via_api_concurrent

MATCH_STATUS_COLUMN = "AIMatchStatus"
MATCH_NAME_COLUMN = "AIMatchedName"
STATUS_MATCHED = "matched"
STATUS_NO_MATCH = "no_match"
STATUS_API_FAILED = "api_failed"


def _usable_name_mask(series):
    """Return rows containing real, nonblank names (not NaN/None)."""
    return series.notna() & series.astype(str).str.strip().ne("")


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


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


def _load_previous_match_state(
    combined_path: Optional[str], unmatched_path: Optional[str]
) -> Tuple[Dict[str, str], Set[str]]:
    """Load only outcomes known to have received a valid LLM response.

    A populated AIMatchedName in combined.csv is a successful match. A no-match is
    reusable only when unmatched.csv explicitly records AIMatchStatus=no_match.
    Old output files without that status are deliberately retried because they
    cannot distinguish a valid no-match from an API failure represented as None.
    """
    matched: Dict[str, str] = {}
    verified_no_match: Set[str] = set()

    if combined_path and os.path.exists(combined_path):
        previous = pd.read_csv(combined_path, low_memory=False)
        if {"CustomerName", MATCH_NAME_COLUMN}.issubset(previous.columns):
            valid = _usable_name_mask(previous["CustomerName"]) & _usable_name_mask(
                previous[MATCH_NAME_COLUMN]
            )
            for customer, match in previous.loc[
                valid, ["CustomerName", MATCH_NAME_COLUMN]
            ].itertuples(index=False, name=None):
                matched[str(customer)] = str(match)

    if unmatched_path and os.path.exists(unmatched_path):
        previous = pd.read_csv(unmatched_path, low_memory=False)
        if {"CustomerName", MATCH_STATUS_COLUMN}.issubset(previous.columns):
            valid = _usable_name_mask(previous["CustomerName"])
            no_match = previous[MATCH_STATUS_COLUMN].astype(str).eq(STATUS_NO_MATCH)
            verified_no_match.update(
                previous.loc[valid & no_match, "CustomerName"].astype(str).tolist()
            )

    return matched, verified_no_match


def combine_data(
    contracts_data,
    mi_data,
    regno_key_pairs,
    previous_combined_path: Optional[str] = None,
    previous_unmatched_path: Optional[str] = None,
):
    """Combine contracts and MI data, incrementally retrying failed API matches."""
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

    valid_mi_customer = _usable_name_mask(mi["CustomerName"])
    _report_missing_customer_names(mi, valid_mi_customer)

    contracts = contracts.merge(
        regno_keys, on="SupplierCompanyRegistrationNumber", how="inner"
    )
    contracts["PairID"] = (
        contracts["SupplierKey"].astype(str) + "+" + contracts["buyer"].str.lower()
    )
    mi["PairID"] = mi["SupplierKey"].astype(str) + "+" + mi["CustomerName"].str.lower()
    contracts_with_mi = contracts.merge(mi, on="PairID", how="left")
    matched_pair_ids = mi["PairID"].isin(contracts_with_mi["PairID"])
    unmatched_mi_all = mi[~matched_pair_ids]
    unmatched_mi_all_valid = unmatched_mi_all.loc[
        _usable_name_mask(unmatched_mi_all["CustomerName"])
    ]
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
    unmatched_mi = unmatched_mi_all_valid[
        ~unmatched_mi_all_valid["CustomerName"].isin(mi_buyer_names_to_ignore)
    ].copy()

    # These columns persist the distinction between a valid LLM no-match and an
    # API/network failure, which a blank AIMatchedName alone cannot represent.
    mi[MATCH_NAME_COLUMN] = pd.NA
    mi[MATCH_STATUS_COLUMN] = pd.NA

    if not unmatched_mi.empty:
        unique_unmatched_customers = unmatched_mi["CustomerName"].astype(str).unique().tolist()
        force_refresh = _env_true("FORCE_REFRESH_MATCHES")
        if force_refresh:
            previous_matches: Dict[str, str] = {}
            previous_no_matches: Set[str] = set()
            print("FORCE_REFRESH_MATCHES=true; ignoring previous match outputs.", flush=True)
        else:
            previous_matches, previous_no_matches = _load_previous_match_state(
                previous_combined_path, previous_unmatched_path
            )

        relevant = set(unique_unmatched_customers)
        previous_matches = {
            name: match for name, match in previous_matches.items() if name in relevant
        }
        previous_no_matches &= relevant
        completed = set(previous_matches) | previous_no_matches
        retry_inputs = [name for name in unique_unmatched_customers if name not in completed]
        print(
            f"Loaded {len(completed)} completed items "
            f"({len(previous_matches)} matched, {len(previous_no_matches)} verified no-match). "
            f"Retrying {len(retry_inputs)} network-failed or unrecorded items...",
            flush=True,
        )

        new_results: Dict[str, Optional[str]] = {}
        failed_items: Set[str] = set()
        if retry_inputs:
            workers = int(os.getenv("MATCH_STRING_MAX_WORKERS", "4"))
            timeout_s = float(os.getenv("MATCH_STRING_TIMEOUT_SECONDS", "60"))
            print(
                f"Total unique unmatched customers = {len(unique_unmatched_customers)}; "
                f"API retry inputs={len(retry_inputs)}, workers={workers}, "
                f"timeout={timeout_s}s, method=POST",
                flush=True,
            )
            new_results = match_strings_via_api_concurrent(
                input_strings=retry_inputs,
                list_of_strings=buyer_names_from_contracts,
                prompt_path=os.getenv("PROMPT_PATH"),
                api_url=os.getenv("NAME_MATCH_API_ENDPOINT"),
                timeout_s=timeout_s,
                max_workers=workers,
                api_method="POST",
                show_progress=True,
                progress_desc="Matching buyers via API",
                failed_items=failed_items,
            )

        all_matches: Dict[str, str] = dict(previous_matches)
        verified_no_matches = set(previous_no_matches)
        for name, match in new_results.items():
            if name in failed_items:
                continue
            if match is None:
                verified_no_matches.add(name)
            else:
                all_matches[name] = match

        customer_names = mi["CustomerName"].astype("string")
        mi[MATCH_NAME_COLUMN] = customer_names.map(all_matches)
        mi.loc[customer_names.isin(all_matches), MATCH_STATUS_COLUMN] = STATUS_MATCHED
        mi.loc[customer_names.isin(verified_no_matches), MATCH_STATUS_COLUMN] = STATUS_NO_MATCH
        mi.loc[customer_names.isin(failed_items), MATCH_STATUS_COLUMN] = STATUS_API_FAILED

        # Only successful matches can create additional contract/MI joins. Valid
        # no-match and failed rows remain in unmatched.csv with distinct statuses.
        ai_matched_mi = mi.loc[
            customer_names.isin(all_matches) & mi[MATCH_NAME_COLUMN].notna()
        ].copy()
        if not ai_matched_mi.empty:
            ai_matched_mi["PairID"] = (
                ai_matched_mi["SupplierKey"].astype("Int64").astype(str)
                + "+"
                + ai_matched_mi[MATCH_NAME_COLUMN].str.lower()
            )
            contracts_with_mi_ai = contracts.merge(ai_matched_mi, on="PairID", how="left")
            contracts_with_mi = pd.concat(
                [contracts_with_mi, contracts_with_mi_ai], ignore_index=True
            )

    matched_pair_ids = mi["PairID"].isin(contracts_with_mi["PairID"])
    unmatched_mi = mi[~matched_pair_ids]
    return contracts_with_mi, unmatched_mi


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--indir", required=True)
    parser.add_argument("--outdir", required=True)
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    combined_path = os.path.join(args.outdir, "combined.csv")
    unmatched_path = os.path.join(args.outdir, "unmatched.csv")

    combined, unmatched = combine_data(
        contracts_data=os.path.join(args.indir, "contracts.csv"),
        mi_data=os.path.join(args.indir, "mi.csv"),
        regno_key_pairs=os.path.join(args.indir, "reg_number_supplier_key.csv"),
        previous_combined_path=combined_path,
        previous_unmatched_path=unmatched_path,
    )
    combined.to_csv(combined_path, index=False)
    unmatched.to_csv(unmatched_path, index=False)
