import math
import os

import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from matplotlib.backends.backend_pdf import PdfPages


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


def plot_undeclared_spend_distribution(df: pd.DataFrame, output_path: str) -> None:
    """
    Plots the distribution of undeclared spend within each Category as a
    histogram, faceted by Category, and writes the result to an A4 PDF with
    8 plots per page.

    Args:
        df: A DataFrame containing at least the columns `Category` and
            `Undeclared Amount (Spend)`.
        output_path: File path for the output PDF.
    """
    # A4 dimensions in inches (landscape gives more space for 2x4 grid)
    A4_LANDSCAPE = (11.69, 8.27)
    PLOTS_PER_PAGE = 8
    COLS = 4
    ROWS = 2

    categories = sorted(df["Category"].dropna().unique())
    n_pages = math.ceil(len(categories) / PLOTS_PER_PAGE)

    with PdfPages(output_path) as pdf:
        for page in range(n_pages):
            page_categories = categories[
                page * PLOTS_PER_PAGE : (page + 1) * PLOTS_PER_PAGE
            ]
            n_plots = len(page_categories)

            fig, axes = plt.subplots(ROWS, COLS, figsize=A4_LANDSCAPE)
            axes_flat = axes.flatten()

            for i, category in enumerate(page_categories):
                ax = axes_flat[i]
                spend = (
                    df.loc[
                        df["Category"] == category, "Undeclared Amount (Spend)"
                    ].dropna()
                    / 1_000_000
                )
                ax.hist(spend, bins=20, edgecolor="white", color="steelblue")
                ax.set_title(category, fontsize=8, wrap=True)
                ax.set_xlabel("Undeclared Spend (£m)", fontsize=7)
                ax.set_ylabel("Count", fontsize=7)
                ax.tick_params(labelsize=6)

            # Hide any unused subplot axes on the last page
            for j in range(n_plots, PLOTS_PER_PAGE):
                axes_flat[j].set_visible(False)

            fig.suptitle(
                f"Distribution of Undeclared Spend by Category (page {page + 1} of {n_pages})",
                fontsize=10,
                y=1.01,
            )
            fig.tight_layout()
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)


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

    # Plot distribution of undeclared spend per category
    output_pdf = "undeclared_spend_distribution.pdf"
    plot_undeclared_spend_distribution(df, output_pdf)
    print(f"\nDistribution plots written to: {output_pdf}")


if __name__ == "__main__":
    main()
