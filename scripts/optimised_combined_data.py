import polars as pl
import os
import sys
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
# Keep your existing import
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
    def check_if_data_exists(path):
        if not os.path.exists(path):
            raise Exception(f"File {path} does not exist")
        return pl.scan_csv(path)


    contracts = check_if_data_exists(contracts_data).with_columns(
        pl.col("SupplierCompanyRegistrationNumber").cast(pl.Utf8)
    )
    mi = check_if_data_exists(mi_data).with_columns(
        pl.col("SupplierKey").cast(pl.Int64)
    )

    regno_keys = check_if_data_exists(regno_key_pairs).with_columns([
        pl.col("SupplierCompanyRegistrationNumber").cast(pl.Utf8),
        pl.col("SupplierKey").cast(pl.Int64)
    ])

    contracts = contracts.join(regno_keys, on="SupplierCompanyRegistrationNumber", how="inner")

    contracts = contracts.with_columns(
        (pl.col("SupplierKey").cast(pl.Utf8) + "+" + pl.col("buyer").str.to_lowercase()).alias("PairID")
    )

    mi= mi.with_columns(
        (pl.col("SupplierKey").cast(pl.Utf8) + "+" + pl.col("CustomerName").str.to_lowercase()).alias("PairID")
    )
    # loads the dataframe now
    contracts = contracts.collect()
    mi = mi.collect()

    contracts_with_mi = contracts.join(mi, on="PairID", how="left")
    matched_pair_ids = mi.filter(pl.col("PairID").is_in(contracts_with_mi["PairID"]))
    unmatched_mi_all = mi.filter(~pl.col("PairID").is_in(contracts_with_mi["PairID"]))

    buyer_names_from_contracts = contracts["buyer"].unique()
    unmatched_mi = unmatched_mi_all.filter(~pl.col("CustomerName").is_in(buyer_names_from_contracts))

    if model and not unmatched_mi.is_empty():
        unique_unmatched_customers = unmatched_mi["CustomerName"].unique().to_list()
        choices = buyer_names_from_contracts.to_list()
        ai_matches = []
        count = 0
        number_of_names_to_match = len(unique_unmatched_customers)
        for i in unique_unmatched_customers:
            matched_name = match_string_with_langchain(i, choices, model, prompt_path="prompts/buyer_match_v2.txt")
            ai_matches.append(matched_name)
            count += 1
            if count % 10 == 0:
                print(f"Processed {count} / {number_of_names_to_match} names")
        name_map_df = pl.DataFrame({
            "CustomerName": unique_unmatched_customers,
            "AIMatchedName": ai_matches
        })

        unmatched_mi = unmatched_mi.join(name_map_df, on="CustomerName", how="left")
        unmatched_mi = unmatched_mi.with_columns(
            pl.format("{}+{}",
                      pl.col("SupplierKey"),
                      pl.col("AIMatchedName").str.to_lowercase()
                      ).alias("PairID")
        )
        unmatched_mi_to_join = unmatched_mi.drop("AIMatchedName")


        contracts_with_mi_AI = contracts.join(unmatched_mi_to_join, on="PairID", how="left")

        contracts_with_mi = pl.concat(
            [contracts_with_mi, contracts_with_mi_AI.select(contracts_with_mi.columns)],
            how="vertical"
        )


        unmatched_mi = mi.filter(~pl.col("PairID").is_in(contracts_with_mi["PairID"]))

    return (contracts_with_mi, unmatched_mi)

if __name__ == "__main__":

    load_dotenv()

    model = AzureChatOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        openai_api_key=os.getenv("AZURE_OPENAI_KEY"),
        azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
        openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION")
    )

    # run this block for live data
    combined, unmatched = combine_data(
        contracts_data="data/contracts.csv",
        mi_data="data/mi.csv",
        regno_key_pairs="data/reg_number_supplier_key.csv",
        model=model
    )
    combined.to_csv("data/combined.csv", index=False)
    unmatched.to_csv("data/unmatched.csv", index=False)

    # # run this block for testing
    # combined, unmatched = combine_data(
    #     contracts_data="dummy_data/dummy_contracts.csv",
    #     mi_data="dummy_data/dummy_mi.csv",
    #     regno_key_pairs="dummy_data/dummy_reg_key_pairs.csv",
    #     model=model
    # )
    # combined.write_csv("dummy_data/dummy_combined_optimised.csv")
    # unmatched.write_csv("dummy_data/dummy_unmatched_mi_optimised.csv")