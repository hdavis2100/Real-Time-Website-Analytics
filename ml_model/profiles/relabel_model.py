from __future__ import annotations

import argparse
from typing import Any

import pandas as pd

from poker_platform.storage import bootstrap_backend
from profiles.labels import label_centroid, summarize_centroid


def relabel_profile_model(
    model_run_id: str | None = None,
    *,
    warehouse: Any | None = None,
) -> dict[str, Any]:
    warehouse = warehouse or bootstrap_backend()
    resolved_model_run_id = model_run_id or _active_model_run_id(warehouse)
    if not resolved_model_run_id:
        return {"status": "not_found", "message": "No active profile model is available"}

    centroids = warehouse.load_table(
        "PROFILE_CLUSTER_CENTROIDS",
        filters_eq={"model_run_id": resolved_model_run_id},
        order_by=[("cluster_id", True)],
    )
    if centroids.empty:
        return {"status": "not_found", "message": f"No centroids found for {resolved_model_run_id}"}

    updated_centroids = centroids.copy()
    label_map: dict[int, str] = {}
    summary_map: dict[int, str] = {}
    changed_clusters = 0
    for index, row in updated_centroids.iterrows():
        centroid = _variant_to_dict(row.get("centroid_json"))
        cluster_id = int(row["cluster_id"])
        new_label = label_centroid(centroid)
        new_summary = summarize_centroid(centroid)
        label_map[cluster_id] = new_label
        summary_map[cluster_id] = new_summary
        if row.get("cluster_label") != new_label or row.get("summary_text") != new_summary:
            changed_clusters += 1
        updated_centroids.at[index, "cluster_label"] = new_label
        updated_centroids.at[index, "summary_text"] = new_summary

    warehouse.delete_matching_keys(
        "PROFILE_CLUSTER_CENTROIDS",
        updated_centroids[["model_run_id", "cluster_id"]],
        ["model_run_id", "cluster_id"],
    )
    warehouse.write_dataframe("PROFILE_CLUSTER_CENTROIDS", updated_centroids, force_bulk=True)

    updated_historical_assignments = _relabel_historical_assignments(
        warehouse,
        model_run_id=resolved_model_run_id,
        label_map=label_map,
    )
    updated_session_results = _relabel_session_results(
        warehouse,
        model_run_id=resolved_model_run_id,
        label_map=label_map,
        summary_map=summary_map,
    )

    return {
        "status": "ready",
        "model_run_id": resolved_model_run_id,
        "changed_clusters": changed_clusters,
        "updated_historical_assignments": updated_historical_assignments,
        "updated_session_results": updated_session_results,
        "labels": label_map,
    }


def _active_model_run_id(warehouse: Any) -> str | None:
    runs = warehouse.load_table(
        "PROFILE_MODEL_RUNS",
        filters_eq={"active": True},
        order_by=[("activated_at", False), ("created_at", False)],
        limit=1,
    )
    if runs.empty:
        return None
    return str(runs.iloc[0]["model_run_id"])


def _relabel_historical_assignments(
    warehouse: Any,
    *,
    model_run_id: str,
    label_map: dict[int, str],
) -> int:
    if hasattr(warehouse, "execute"):
        warehouse.execute(
            "UPDATE PROFILE_HISTORICAL_ASSIGNMENTS AS target "
            "SET CLUSTER_LABEL = source.CLUSTER_LABEL "
            "FROM PROFILE_CLUSTER_CENTROIDS AS source "
            f"WHERE target.MODEL_RUN_ID = {_sql_literal(model_run_id)} "
            "AND target.MODEL_RUN_ID = source.MODEL_RUN_ID "
            "AND target.CLUSTER_ID = source.CLUSTER_ID"
        )
        count_frame = warehouse.query_df(
            "SELECT COUNT(*) AS row_count FROM PROFILE_HISTORICAL_ASSIGNMENTS "
            f"WHERE MODEL_RUN_ID = {_sql_literal(model_run_id)} AND CLUSTER_ID IS NOT NULL"
        )
        return int(count_frame.iloc[0]["row_count"]) if not count_frame.empty else 0

    assignments = warehouse.load_table(
        "PROFILE_HISTORICAL_ASSIGNMENTS",
        filters_eq={"model_run_id": model_run_id},
    )
    if assignments.empty:
        return 0
    updated = assignments.copy()
    updated["cluster_label"] = updated["cluster_id"].map(
        lambda value: label_map.get(int(value), None) if pd.notna(value) else None
    ).fillna(updated["cluster_label"])
    warehouse.delete_matching_keys(
        "PROFILE_HISTORICAL_ASSIGNMENTS",
        updated[["model_run_id", "player_id"]],
        ["model_run_id", "player_id"],
    )
    warehouse.write_dataframe("PROFILE_HISTORICAL_ASSIGNMENTS", updated, force_bulk=True)
    return int(len(updated))


def _relabel_session_results(
    warehouse: Any,
    *,
    model_run_id: str,
    label_map: dict[int, str],
    summary_map: dict[int, str],
) -> int:
    if hasattr(warehouse, "execute"):
        warehouse.execute(
            "UPDATE PROFILE_SESSION_RESULTS AS target "
            "SET CLUSTER_LABEL = source.CLUSTER_LABEL, SUMMARY_TEXT = source.SUMMARY_TEXT "
            "FROM PROFILE_CLUSTER_CENTROIDS AS source "
            f"WHERE target.MODEL_RUN_ID = {_sql_literal(model_run_id)} "
            "AND target.MODEL_RUN_ID = source.MODEL_RUN_ID "
            "AND target.CLUSTER_ID = source.CLUSTER_ID"
        )
        count_frame = warehouse.query_df(
            "SELECT COUNT(*) AS row_count FROM PROFILE_SESSION_RESULTS "
            f"WHERE MODEL_RUN_ID = {_sql_literal(model_run_id)} AND CLUSTER_ID IS NOT NULL"
        )
        return int(count_frame.iloc[0]["row_count"]) if not count_frame.empty else 0

    results = warehouse.load_table(
        "PROFILE_SESSION_RESULTS",
        filters_eq={"model_run_id": model_run_id},
    )
    if results.empty:
        return 0
    updated = results.copy()
    valid_mask = updated["cluster_id"].notna()
    if not valid_mask.any():
        return 0
    updated.loc[valid_mask, "cluster_label"] = updated.loc[valid_mask, "cluster_id"].map(
        lambda value: label_map.get(int(value), None) if pd.notna(value) else None
    )
    updated.loc[valid_mask, "summary_text"] = updated.loc[valid_mask, "cluster_id"].map(
        lambda value: summary_map.get(int(value), None) if pd.notna(value) else None
    )
    warehouse.delete_matching_keys(
        "PROFILE_SESSION_RESULTS",
        updated[["model_run_id", "source_run_id", "simulation_run_id", "subject_id"]],
        ["model_run_id", "source_run_id", "simulation_run_id", "subject_id"],
    )
    warehouse.write_dataframe("PROFILE_SESSION_RESULTS", updated, force_bulk=True)
    return int(valid_mask.sum())


def _variant_to_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, "", {}):
        return {}
    if isinstance(value, str):
        import json

        parsed = json.loads(value)
        if isinstance(parsed, str):
            parsed = json.loads(parsed)
        return parsed if isinstance(parsed, dict) else {}
    try:
        return dict(value)
    except Exception:
        return {}


def _sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Recompute human-readable labels for a trained profile model.")
    parser.add_argument("--model-run-id", help="Profile model run id. Defaults to the active model.")
    args = parser.parse_args(argv)
    result = relabel_profile_model(args.model_run_id)
    print(result)
    return 0 if result.get("status") == "ready" else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
