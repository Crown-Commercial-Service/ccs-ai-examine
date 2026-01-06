from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import random

import pandas as pd
import mlflow
mlflow.set_tracking_uri("file:./mlruns")

from utils import match_string_with_langchain
from evaluation.mock_langchain_model import MockChatModelWithCandidates

# Optional: fallback mock model
class _MockResponse:
    def __init__(self, content: str):
        self.content = content


def _find_col(df: pd.DataFrame, name: str) -> str:
    """
    Find a column case-insensitively, allowing minor differences.
    """
    target = name.strip().lower()
    mapping = {c.strip().lower(): c for c in df.columns}
    if target in mapping:
        return mapping[target]
    raise ValueError(f"Missing required column '{name}'. Found columns: {list(df.columns)}")


def normalise_prediction(pred: str) -> str:
    """
    Normalise model output to:
      - "None" OR
      - exact candidate string (without wrapping quotes)
    """
    p = (pred or "").strip()

    # remove wrapping quotes if present
    if (p.startswith('"') and p.endswith('"')) or (p.startswith("'") and p.endswith("'")):
        p = p[1:-1].strip()

    if p.lower() in {"none", "null", "n/a", "na", ""}:
        return "None"

    return p


def is_negative_control(error_type: str, ground_truth: str) -> bool:
    """
    Determine whether a row is a negative control case.
    We treat either:
      - Error Type starts with "Negative control"
      - or ground truth is N/A/None/empty
    as negative controls.
    """
    et = (error_type or "").strip().lower()
    gt = (ground_truth or "").strip().lower()
    if et.startswith("negative control"):
        return True
    if gt in {"n/a", "na", "none", ""}:
        return True
    return False



def build_candidate_list(
    input_name: str,
    ground_truth: str,
    all_candidates: List[str],
    num_distractors: int,
    seed: int,
    is_negative: bool,
    ) -> List[str]:
    """
    Build a realistic candidate list for evaluation.
    """
    rng = random.Random(seed + hash(input_name) % 1_000_000)

    pool = [c for c in all_candidates if c.strip()]
    pool_no_gt = [c for c in pool if c != ground_truth]

    if is_negative:
        k = min(len(pool_no_gt), num_distractors)
        cands = rng.sample(pool_no_gt, k=k)
        rng.shuffle(cands)
        return cands

    # positive case
    k = min(len(pool_no_gt), num_distractors)
    distractors = rng.sample(pool_no_gt, k=k)
    cands = [ground_truth] + distractors
    rng.shuffle(cands)
    return cands


def evaluate_prompt_on_benchmark(
    df: pd.DataFrame,
    prompt_path: str,
    experiment_name: str = "EXAMINE_name_matching_updated_scripts",
    similarity_threshold: float = 0.85,
    num_distractors: int = 20,
    seed: int = 42,
    ) -> Dict[str, Any]:

    """
    Evaluate one prompt file on the benchmark dataset and log to MLflow as one run.
    """
    input_col = _find_col(df, "Input Name")
    gt_col = _find_col(df, "Match Option")
    err_col = _find_col(df, "Error Type")
    ent_col = _find_col(df, "Entity Type")

    # Build candidate pool from all non-negative match options in the dataset
    all_candidates = sorted({
        str(x).strip()
        for x in df[gt_col].astype(str).tolist()
        if str(x).strip().lower() not in {"n/a", "na", "none", ""}
    })

    run_name = f"{Path(prompt_path).stem}_mock_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=run_name):
        # Params
        mlflow.log_param("prompt_file", Path(prompt_path).name)
        mlflow.log_param("prompt_path", prompt_path)
        mlflow.log_param("llm_type", "MockChatModelWithCandidates")
        mlflow.log_param("candidate_pool_size", len(all_candidates))
        mlflow.log_param("similarity_threshold", similarity_threshold)
        mlflow.log_param("num_distractors", num_distractors)
        mlflow.log_param("seed", seed)


        rows = []
        for _, r in df.iterrows():
            input_name = str(r[input_col]).strip()
            ground_truth = str(r[gt_col]).strip()
            error_type = str(r[err_col]).strip()
            entity_type = str(r[ent_col]).strip()

            neg = is_negative_control(error_type, ground_truth)

            # Candidate list strategy:
            # Use the full pool for all rows (simulates real retrieval)
            candidates = build_candidate_list(
                            input_name=input_name,
                            ground_truth=ground_truth,
                            all_candidates=all_candidates,
                            num_distractors=num_distractors,
                            seed=seed,
                            is_negative=neg,
                        )


            model = MockChatModelWithCandidates(
                candidates=candidates,
                similarity_threshold=similarity_threshold
            )

            pred_raw = match_string_with_langchain(
                input_string=input_name,
                list_of_strings=candidates,
                model=model,
                prompt_path=prompt_path,
            )

            pred = normalise_prediction(pred_raw)

            if neg:
                correct = (pred == "None")
            else:
                # must match exactly the ground truth string
                correct = (pred == ground_truth)

            rows.append({
                "input_name": input_name,
                "ground_truth": ground_truth,
                "prediction": pred,
                "correct": int(correct),
                "error_type": error_type,
                "entity_type": entity_type,
                "is_negative_control": int(neg),
            })

        out = pd.DataFrame(rows)

        # Metrics
        acc_overall = float(out["correct"].mean())
        mlflow.log_metric("accuracy_overall", acc_overall)

        # By error type
        by_err = out.groupby("error_type")["correct"].mean().to_dict()
        for k, v in by_err.items():
            safe = k.lower().replace(" ", "_").replace("-", "_")
            mlflow.log_metric(f"accuracy_error_{safe}", float(v))

        # By entity type
        by_ent = out.groupby("entity_type")["correct"].mean().to_dict()
        for k, v in by_ent.items():
            safe = k.lower().replace(" ", "_").replace("-", "_")
            mlflow.log_metric(f"accuracy_entity_{safe}", float(v))

        # Artifacts
        artifacts_dir = Path("mlflow_outputs") / run_name
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        pred_path = artifacts_dir / "predictions.csv"
        out.to_csv(pred_path, index=False)

        summary = {
            "prompt_file": Path(prompt_path).name,
            "accuracy_overall": acc_overall,
            "accuracy_by_error_type": by_err,
            "accuracy_by_entity_type": by_ent,
            "rows": int(len(out)),
            "candidate_pool_size": len(all_candidates),
            "similarity_threshold": similarity_threshold,
        }

        summary_path = artifacts_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        # log artifacts into MLflow
        mlflow.log_artifact(str(pred_path))
        mlflow.log_artifact(str(summary_path))
        mlflow.log_artifact(prompt_path)

        print(f"\nPROMPT: {Path(prompt_path).name}\nSUMMARY: {summary}")
        return summary


def main():
    # Paths (adjust if your dataset lives somewhere else)
    dataset_path = Path("benchmark_data") / "ccs_combined_buyer_supplier_benchmark.csv"
    if not dataset_path.exists():
        # fallback: if the data in root
        dataset_path = Path("ccs_combined_buyer_supplier_benchmark.csv")

    if not dataset_path.exists():
        raise FileNotFoundError(
            "Benchmark dataset not found. Expected:\n"
            "  benchmark_data/ccs_combined_buyer_supplier_benchmark.csv\n"
            "or\n"
            "  ccs_combined_buyer_supplier_benchmark.csv"
        )

    df = pd.read_csv(dataset_path).fillna("")

    prompt_files = sorted(Path("prompts").glob("buyer_match_v*.txt"))
    if not prompt_files:
        raise FileNotFoundError("No prompt files found. Expected prompts/buyer_match_v*.txt")

    for p in prompt_files:
        evaluate_prompt_on_benchmark( df,str(p),num_distractors=20,seed=42,)



if __name__ == "__main__":
    main()
