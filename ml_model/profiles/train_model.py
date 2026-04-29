from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import uuid

import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

from poker_platform.storage import bootstrap_backend
from profiles.constants import (
    CANDIDATE_CLUSTER_COUNTS,
    DEFAULT_TRAINING_DATASET,
    FEATURE_COLUMNS,
    FEATURE_VERSION,
    MIN_TRAINING_HANDS,
)
from profiles.labels import label_centroid, summarize_centroid

MIN_ACTIVATION_TRAINING_PLAYERS = 100


def train_historical_archetype_model(
    *,
    training_dataset: str = DEFAULT_TRAINING_DATASET,
    feature_version: str = FEATURE_VERSION,
    min_hands: int = MIN_TRAINING_HANDS,
    model_run_id: str | None = None,
    activate: bool = True,
) -> dict[str, int | str | list[dict[str, float | int | str]]]:
    warehouse = bootstrap_backend()
    training_frame = warehouse.load_table(
        "PROFILE_PLAYER_TRAINING_FEATURES",
        filters_eq={
            "training_dataset": training_dataset,
            "feature_version": feature_version,
        },
    )
    if not training_frame.empty:
        training_frame = training_frame[training_frame["hands_played"].fillna(0).astype(int) >= int(min_hands)].copy()
        if "eligible_for_training" in training_frame.columns:
            training_frame = training_frame[training_frame["eligible_for_training"].fillna(False).astype(bool)].copy()
    if training_frame.empty:
        return {
            "status": "not_ready",
            "message": f"No eligible training players found for {training_dataset}/{feature_version}",
        }

    model_run_id = model_run_id or f"profile_model_{uuid.uuid4().hex[:10]}"
    matrix = training_frame[FEATURE_COLUMNS].fillna(0.0)
    scaler = StandardScaler()
    scaled = scaler.fit_transform(matrix)
    candidate_results: list[dict[str, float | int | str]] = []
    selected = _select_cluster_count(scaled, candidate_results)
    final_model = MiniBatchKMeans(n_clusters=selected, random_state=42, batch_size=1024, n_init="auto")
    labels = final_model.fit_predict(scaled)
    training_frame = training_frame.copy()
    training_frame["cluster_id"] = labels
    training_frame["distance_to_centroid"] = _distances_to_assigned_centroid(scaled, final_model.cluster_centers_, labels)

    raw_centroids = scaler.inverse_transform(final_model.cluster_centers_)
    centroid_rows: list[dict[str, object]] = []
    prototype_rows: list[dict[str, object]] = []
    assignment_rows: list[dict[str, object]] = []
    feature_rows: list[dict[str, object]] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    activation_skipped_reason: str | None = None

    cluster_sizes = training_frame["cluster_id"].value_counts().sort_index()
    total_players = len(training_frame)
    for cluster_id in sorted(cluster_sizes.index.tolist()):
        centroid_vector = raw_centroids[int(cluster_id)]
        centroid_payload = {column: float(centroid_vector[index]) for index, column in enumerate(FEATURE_COLUMNS)}
        centroid_rows.append(
            {
                "model_run_id": model_run_id,
                "cluster_id": int(cluster_id),
                "cluster_label": label_centroid(centroid_payload),
                "summary_text": summarize_centroid(centroid_payload),
                "cluster_size": int(cluster_sizes[cluster_id]),
                "cluster_share": float(cluster_sizes[cluster_id] / total_players),
                "centroid_json": centroid_payload,
                "updated_at": now,
            }
        )

        cluster_members = training_frame[training_frame["cluster_id"] == cluster_id].copy()
        cluster_members = cluster_members.sort_values(["distance_to_centroid", "hands_played", "player_id"], ascending=[True, False, True])
        for prototype_rank, (_, member) in enumerate(cluster_members.head(5).iterrows(), start=1):
            prototype_rows.append(
                {
                    "model_run_id": model_run_id,
                    "cluster_id": int(cluster_id),
                    "prototype_rank": prototype_rank,
                    "player_id": member["player_id"],
                    "source_dataset": member["source_dataset"],
                    "hands_played": int(member["hands_played"]),
                    "distance_to_centroid": float(member["distance_to_centroid"]),
                    "prototype_json": {
                        "player_id": member["player_id"],
                        "source_dataset": member["source_dataset"],
                        "hands_played": int(member["hands_played"]),
                        "feature_vector": {column: float(member[column]) for column in FEATURE_COLUMNS},
                    },
                    "updated_at": now,
                }
            )

    for order, column in enumerate(FEATURE_COLUMNS, start=1):
        column_values = matrix[column].astype(float)
        feature_rows.append(
            {
                "model_run_id": model_run_id,
                "feature_name": column,
                "feature_order": order,
                "feature_version": feature_version,
                "scaler_mean": float(scaler.mean_[order - 1]),
                "scaler_scale": float(max(scaler.scale_[order - 1], 1e-9)),
                "global_mean": float(column_values.mean()),
                "updated_at": now,
            }
        )

    cluster_labels = {row["cluster_id"]: row["cluster_label"] for row in centroid_rows}
    for _, row in training_frame.iterrows():
        assignment_rows.append(
            {
                "model_run_id": model_run_id,
                "player_id": row["player_id"],
                "source_dataset": row["source_dataset"],
                "cluster_id": int(row["cluster_id"]),
                "cluster_label": cluster_labels[int(row["cluster_id"])],
                "distance_to_centroid": float(row["distance_to_centroid"]),
                "hands_played": int(row["hands_played"]),
                "assigned_at": now,
            }
        )

    if activate and total_players < MIN_ACTIVATION_TRAINING_PLAYERS:
        activation_skipped_reason = (
            "Activation skipped because "
            f"training_player_count={int(total_players)} is below "
            f"the safety threshold of {MIN_ACTIVATION_TRAINING_PLAYERS}"
        )
        activate = False

    if activate:
        deactivate_active_profile_models(warehouse)

    model_run_row = pd.DataFrame(
        [
            {
                "model_run_id": model_run_id,
                "created_at": now,
                "activated_at": now if activate else None,
                "active": activate,
                "algorithm": "minibatch_kmeans",
                "cluster_count": int(selected),
                "training_dataset": training_dataset,
                "feature_version": feature_version,
                "training_player_count": int(total_players),
                "min_hand_threshold": int(min_hands),
                "parameters_json": {
                    "candidate_cluster_counts": CANDIDATE_CLUSTER_COUNTS,
                    "random_state": 42,
                    "batch_size": 1024,
                    "feature_columns": FEATURE_COLUMNS,
                },
                "selection_metrics_json": {"candidates": candidate_results, "selected_k": int(selected)},
                "status": "active" if activate else "trained",
            }
        ]
    )

    _replace_rows(warehouse, "PROFILE_MODEL_RUNS", model_run_row, ["model_run_id"])
    _replace_rows(warehouse, "PROFILE_MODEL_FEATURES", pd.DataFrame(feature_rows), ["model_run_id", "feature_name"])
    _replace_rows(warehouse, "PROFILE_CLUSTER_CENTROIDS", pd.DataFrame(centroid_rows), ["model_run_id", "cluster_id"])
    _replace_rows(
        warehouse,
        "PROFILE_CLUSTER_PROTOTYPES",
        pd.DataFrame(prototype_rows),
        ["model_run_id", "cluster_id", "prototype_rank"],
    )
    _replace_rows(
        warehouse,
        "PROFILE_HISTORICAL_ASSIGNMENTS",
        pd.DataFrame(assignment_rows),
        ["model_run_id", "player_id"],
    )

    return {
        "status": "ready",
        "model_run_id": model_run_id,
        "training_dataset": training_dataset,
        "feature_version": feature_version,
        "training_player_count": int(total_players),
        "cluster_count": int(selected),
        "candidate_results": candidate_results,
        "active": bool(activate),
        "activation_skipped_reason": activation_skipped_reason,
    }


def _select_cluster_count(scaled: np.ndarray, candidate_results: list[dict[str, float | int | str]]) -> int:
    n_players = len(scaled)
    distinct_rows = max(1, len(np.unique(scaled, axis=0)))
    viable = [k for k in CANDIDATE_CLUSTER_COUNTS if 1 < k <= n_players and k <= distinct_rows]
    if not viable:
        return max(1, min(8, n_players, distinct_rows))

    best_k: int | None = None
    best_score = float("-inf")
    fallback_k = viable[0]
    fallback_score = float("-inf")
    for k in viable:
        model = MiniBatchKMeans(n_clusters=k, random_state=42, batch_size=1024, n_init="auto")
        labels = model.fit_predict(scaled)
        share = float(pd.Series(labels).value_counts(normalize=True).min())
        score = float(silhouette_score(scaled, labels)) if len(set(labels.tolist())) > 1 else float("-inf")
        candidate_results.append(
            {
                "cluster_count": int(k),
                "silhouette_score": score,
                "min_cluster_share": share,
                "accepted": share >= 0.03,
            }
        )
        if score > fallback_score or (score == fallback_score and k < fallback_k):
            fallback_k = k
            fallback_score = score
        if share < 0.03:
            continue
        if score > best_score or (score == best_score and (best_k is None or k < best_k)):
            best_k = k
            best_score = score

    return best_k or fallback_k


def _distances_to_assigned_centroid(scaled: np.ndarray, centroids: np.ndarray, labels: np.ndarray) -> np.ndarray:
    distances = np.empty(len(labels), dtype=float)
    for index, label in enumerate(labels):
        distances[index] = float(np.linalg.norm(scaled[index] - centroids[label]))
    return distances


def _replace_rows(warehouse, table: str, frame: pd.DataFrame, key_columns: list[str]) -> None:
    if frame.empty:
        return
    warehouse.delete_matching_keys(table, frame, key_columns)
    warehouse.write_dataframe(table, frame, force_bulk=True)


def deactivate_active_profile_models(warehouse) -> int:
    active_runs = warehouse.load_table("PROFILE_MODEL_RUNS", filters_eq={"active": True})
    if active_runs.empty:
        return 0

    updates = active_runs.copy()
    updates["active"] = False
    if "status" in updates.columns:
        updates["status"] = "trained"
    _replace_rows(warehouse, "PROFILE_MODEL_RUNS", updates, ["model_run_id"])
    return int(len(updates))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Train the historical human archetype model.")
    parser.add_argument("--training-dataset", default=DEFAULT_TRAINING_DATASET)
    parser.add_argument("--feature-version", default=FEATURE_VERSION)
    parser.add_argument("--min-hands", type=int, default=MIN_TRAINING_HANDS)
    parser.add_argument("--model-run-id", default=None)
    parser.add_argument("--no-activate", action="store_true")
    args = parser.parse_args(argv)

    result = train_historical_archetype_model(
        training_dataset=args.training_dataset,
        feature_version=args.feature_version,
        min_hands=args.min_hands,
        model_run_id=args.model_run_id,
        activate=not args.no_activate,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
