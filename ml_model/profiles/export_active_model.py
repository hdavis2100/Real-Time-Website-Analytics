from __future__ import annotations

import argparse
import json
import os
from typing import Any

from poker_platform.config import get_config
from poker_platform.storage import bootstrap_backend, get_warehouse
from profiles.constants import FEATURE_COLUMNS


def load_active_model_artifact(warehouse: Any | None = None) -> dict[str, Any]:
    warehouse = _warehouse_or_default(warehouse)
    runs = warehouse.load_table(
        "PROFILE_MODEL_RUNS",
        filters_eq={"active": True},
        order_by=[("activated_at", False), ("created_at", False)],
        limit=1,
    )
    if runs.empty:
        return {"status": "not_found", "message": "No active profile model is available"}

    run = runs.iloc[0].to_dict()
    model_run_id = str(run["model_run_id"])
    features = warehouse.load_table(
        "PROFILE_MODEL_FEATURES",
        filters_eq={"model_run_id": model_run_id},
        order_by=[("feature_order", True)],
    )
    centroids = warehouse.load_table(
        "PROFILE_CLUSTER_CENTROIDS",
        filters_eq={"model_run_id": model_run_id},
        order_by=[("cluster_id", True)],
    )
    prototypes = warehouse.load_table(
        "PROFILE_CLUSTER_PROTOTYPES",
        filters_eq={"model_run_id": model_run_id},
        order_by=[("cluster_id", True), ("prototype_rank", True)],
    )
    if features.empty or centroids.empty:
        return {"status": "not_found", "message": f"Active model {model_run_id} is missing feature stats or centroids"}

    feature_rows = []
    for row in features.to_dict(orient="records"):
        feature_rows.append(
            {
                "feature_name": row["feature_name"],
                "feature_order": int(row["feature_order"]),
                "scaler_mean": float(row["scaler_mean"]),
                "scaler_scale": float(row["scaler_scale"]),
                "global_mean": float(row["global_mean"]),
            }
        )
    feature_rows.sort(key=lambda row: row["feature_order"])

    centroid_rows = []
    for row in centroids.to_dict(orient="records"):
        centroid_rows.append(
            {
                "cluster_id": int(row["cluster_id"]),
                "cluster_label": row["cluster_label"],
                "summary_text": row["summary_text"],
                "cluster_size": int(row["cluster_size"]),
                "cluster_share": float(row["cluster_share"]),
                "centroid": _variant_to_dict(row["centroid_json"]),
            }
        )

    prototype_rows = []
    if not prototypes.empty:
        for row in prototypes.to_dict(orient="records"):
            prototype_rows.append(
                {
                    "cluster_id": int(row["cluster_id"]),
                    "prototype_rank": int(row["prototype_rank"]),
                    "player_id": row["player_id"],
                    "source_dataset": row["source_dataset"],
                    "hands_played": int(row["hands_played"]),
                    "distance_to_centroid": float(row["distance_to_centroid"]),
                    "prototype": _variant_to_dict(row["prototype_json"]),
                }
            )

    return {
        "status": "ready",
        "model_run_id": model_run_id,
        "training_dataset": run["training_dataset"],
        "feature_version": run["feature_version"],
        "cluster_count": int(run["cluster_count"]),
        "feature_columns": [row["feature_name"] for row in feature_rows] or FEATURE_COLUMNS,
        "feature_stats": feature_rows,
        "centroids": centroid_rows,
        "prototypes": prototype_rows,
        "scoring_artifact": {
            "model_run_id": model_run_id,
            "training_dataset": run["training_dataset"],
            "feature_version": run["feature_version"],
            "feature_columns": [row["feature_name"] for row in feature_rows] or FEATURE_COLUMNS,
            "feature_stats": feature_rows,
            "centroids": centroid_rows,
        },
    }


def _variant_to_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, "", {}):
        return {}
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, str):
            parsed = json.loads(parsed)
        return parsed if isinstance(parsed, dict) else {}
    try:
        return dict(value)
    except Exception:
        return {}


def _warehouse_or_default(warehouse: Any | None = None):
    if warehouse is not None:
        return warehouse
    if os.getenv("POKER_PLATFORM_SKIP_SCHEMA_BOOTSTRAP") == "1":
        return get_warehouse(get_config())
    return bootstrap_backend()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export the active profile scoring artifact as JSON.")
    parser.parse_args(argv)
    print(json.dumps(load_active_model_artifact(), indent=2, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
