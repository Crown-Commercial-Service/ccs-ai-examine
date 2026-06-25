import math
import os

import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from matplotlib.backends.backend_pdf import PdfPages


def summarise_undeclared_spend_by_category(
    df: pd.DataFrame, output_path: str = "results/undeclared_spend_by_category.csv"
) -> pd.DataFrame:
    """
    Summarises total undeclared spend grouped by Category.

    Steps:
    1. Sort rows by the `Category` column.
    2. Group by `Category`.
    3. Sum the `Undeclared Amount (Spend)` for each group.
    4. Sort the result in descending order of `Undeclared Amount (Spend)`.
    5. Write the summary to a CSV file at `output_path`.

    Args:
        df: A DataFrame containing at least the columns `Category`,
            `Undeclared Amount (Spend)`, and `Supplier`.
        output_path: File path for the output CSV. Defaults to
            ``results/undeclared_spend_by_category.csv``.

    Returns:
        A DataFrame with one row per Category, indexed by Category, with the
        total `Undeclared Amount (Spend)` and `Number of Suppliers` sorted
        highest-first by spend.
    """
    sorted_df = df.sort_values("Category")
    grouped = sorted_df.groupby("Category")
    summary = (
        grouped["Undeclared Amount (Spend)"]
        .sum()
        .reset_index()
        .sort_values("Undeclared Amount (Spend)", ascending=False)
    )
    supplier_counts = (
        grouped["Supplier"].nunique().reset_index(name="Number of Suppliers")
    )
    summary = summary.merge(supplier_counts, on="Category")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    summary.to_csv(output_path, index=False)
    return summary


def plot_undeclared_spend_faceted(
    df: pd.DataFrame,
    output_path: str = "results/undeclared_spend_faceted.pdf",
) -> None:
    """
    Plots the distribution of undeclared spend within each Category as a
    histogram, faceted by Category, and writes the result to an A4 PDF with
    8 plots per page.

    Args:
        df: A DataFrame containing at least the columns `Category` and
            `Undeclared Amount (Spend)`.
        output_path: File path for the output PDF. Defaults to
            ``results/undeclared_spend_faceted.pdf``.
    """
    # A4 dimensions in inches (landscape gives more space for 2x4 grid)
    A4_LANDSCAPE = (11.69, 8.27)
    PLOTS_PER_PAGE = 8
    COLS = 4
    ROWS = 2

    categories = sorted(df["Category"].dropna().unique())
    n_pages = math.ceil(len(categories) / PLOTS_PER_PAGE)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
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


def plot_top_n_undeclared_spend(
    df: pd.DataFrame,
    summary: pd.DataFrame,
    top_n: int = 3,
    output_path: str = "results/undeclared_spend_top_n.svg",
) -> None:
    """
    Plots interleaved histograms of undeclared spend for the top-N categories
    by total undeclared spend, on a single axes, and writes the result to an SVG.

    Args:
        df: A DataFrame containing at least the columns `Category` and
            `Undeclared Amount (Spend)`.
        summary: A DataFrame as returned by
            :func:`summarise_undeclared_spend_by_category`, with columns
            ``Category`` and ``Undeclared Amount (Spend)`` sorted
            highest-first.
        top_n: Number of top categories to include. Defaults to ``3``.
        output_path: File path for the output SVG. Defaults to
            ``results/undeclared_spend_top_n.svg``.
    """
    top_categories = summary.head(top_n)["Category"].tolist()

    fig, ax = plt.subplots(figsize=(10, 6))
    for category in top_categories:
        spend = (
            df.loc[df["Category"] == category, "Undeclared Amount (Spend)"].dropna()
            / 1_000_000
        )
        ax.hist(spend, bins=20, alpha=0.5, edgecolor="white", label=category)

    ax.set_title(
        f"Distribution of Undeclared Spend — Top {top_n} Categories by Total Spend",
        fontsize=11,
    )
    ax.set_xlabel("Undeclared Spend (£m)", fontsize=10)
    ax.set_ylabel("Count", fontsize=10)
    ax.legend(title="Category", fontsize=8)
    fig.tight_layout()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, format="svg", bbox_inches="tight")
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

    # Plot faceted distribution of undeclared spend per category
    faceted_pdf = "results/undeclared_spend_faceted.pdf"
    plot_undeclared_spend_faceted(df, faceted_pdf)
    print(f"\nFaceted distribution plots written to: {faceted_pdf}")

    # Plot interleaved distribution for the top-N categories
    top_n_svg = "results/undeclared_spend_top_n.svg"
    plot_top_n_undeclared_spend(df, summary, top_n=3, output_path=top_n_svg)
    print(f"Top-N interleaved distribution plot written to: {top_n_svg}")


if __name__ == "__main__":
    main()
