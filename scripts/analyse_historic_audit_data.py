import os
import pandas as pd
from dotenv import load_dotenv


def summarise_undeclared_spend_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarises total undeclared spend grouped by Category.

    Steps:
    1. Sort rows by the `Category` column.
    2. Group by `Category`.
    3. Sum the `Undeclared Amount (Spend)` for each group.
    4. Sort the result in descending order of `Undeclared Amount (Spend)`.

    Args:
        df: A DataFrame containing at least the columns `Category` and
            `Undeclared Amount (Spend)`.

    Returns:
        A DataFrame with one row per Category, indexed by Category, with the
        total `Undeclared Amount (Spend)` sorted highest-first.
    """
    sorted_df = df.sort_values("Category")
    summary = (
        sorted_df.groupby("Category")["Undeclared Amount (Spend)"]
        .sum()
        .reset_index()
        .sort_values("Undeclared Amount (Spend)", ascending=False)
    )
    return summary


def main():
    # Load environment variables from .env file
    load_dotenv()

    # Get the audit data path from environment variables
    audit_data_path = os.getenv("AUDIT_DATA_PATH")

    if not audit_data_path:
        raise ValueError(
            "AUDIT_DATA_PATH environment variable is not set in the environment or .env file."
        )

    print(f"Reading audit data from: {audit_data_path}")

    # Read the "Undeclared 2024-2025" tab of the excel file
    df = pd.read_excel(audit_data_path, sheet_name="Undeclared 24- 25", header=4)

    # Summarise undeclared spend by category
    print("\nUndeclared spend by category (descending):")
    summary = summarise_undeclared_spend_by_category(df)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
