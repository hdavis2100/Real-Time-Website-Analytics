from __future__ import annotations

import json
from typing import Any

import numpy as np

from profiles.constants import FEATURE_COLUMNS


def load_scoring_artifact(payload: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload, str):
        artifact = json.loads(payload)
    else:
        artifact = payload
    feature_columns = artifact.get("feature_columns") or FEATURE_COLUMNS
    feature_stats = artifact.get("feature_stats") or []
    if not feature_stats:
        raise RuntimeError("Scoring artifact is missing feature stats")
    centroids = artifact.get("centroids") or []
    if not centroids:
        raise RuntimeError("Scoring artifact is missing centroids")
    artifact["feature_columns"] = feature_columns
    return artifact


def score_feature_vector(feature_vector: dict[str, float], artifact: dict[str, Any]) -> dict[str, Any]:
    feature_columns = artifact["feature_columns"]
    feature_stats = {row["feature_name"]: row for row in artifact["feature_stats"]}
    raw_vector = np.array([float(feature_vector.get(column, 0.0)) for column in feature_columns], dtype=float)
    mean_vector = np.array([float(feature_stats[column]["scaler_mean"]) for column in feature_columns], dtype=float)
    scale_vector = np.array([max(float(feature_stats[column]["scaler_scale"]), 1e-9) for column in feature_columns], dtype=float)
    global_mean_vector = np.array([float(feature_stats[column]["global_mean"]) for column in feature_columns], dtype=float)
    scaled_vector = (raw_vector - mean_vector) / scale_vector

    candidates: list[dict[str, Any]] = []
    for centroid in artifact["centroids"]:
        centroid_raw = np.array([float(centroid["centroid"][column]) for column in feature_columns], dtype=float)
        centroid_scaled = (centroid_raw - mean_vector) / scale_vector
        distance = float(np.linalg.norm(scaled_vector - centroid_scaled))
        candidates.append(
            {
                "cluster_id": int(centroid["cluster_id"]),
                "cluster_label": centroid["cluster_label"],
                "summary_text": centroid["summary_text"],
                "distance_to_centroid": distance,
                "centroid_raw": centroid_raw,
            }
        )
    candidates.sort(key=lambda row: row["distance_to_centroid"])
    best = candidates[0]
    second_distance = candidates[1]["distance_to_centroid"] if len(candidates) > 1 else best["distance_to_centroid"] + 1.0
    top_feature_deltas = build_top_feature_deltas(
        feature_columns=feature_columns,
        feature_vector=raw_vector,
        centroid_vector=best["centroid_raw"],
        global_mean_vector=global_mean_vector,
        limit=5,
    )
    return {
        "model_run_id": artifact["model_run_id"],
        "feature_version": artifact["feature_version"],
        "cluster_id": best["cluster_id"],
        "cluster_label": best["cluster_label"],
        "summary_text": best["summary_text"],
        "distance_to_centroid": best["distance_to_centroid"],
        "confidence_score": confidence_score(
            hands_observed=int(feature_vector.get("hands_observed", 0)),
            nearest_distance=best["distance_to_centroid"],
            second_distance=second_distance,
        ),
        "feature_vector": {column: float(value) for column, value in zip(feature_columns, raw_vector, strict=True)},
        "top_feature_deltas": top_feature_deltas,
    }


def build_top_feature_deltas(
    *,
    feature_columns: list[str],
    feature_vector: np.ndarray,
    centroid_vector: np.ndarray,
    global_mean_vector: np.ndarray,
    limit: int,
) -> list[dict[str, float | str]]:
    deltas: list[dict[str, float | str]] = []
    for index, column in enumerate(feature_columns):
        delta_vs_centroid = float(feature_vector[index] - centroid_vector[index])
        delta_vs_global = float(feature_vector[index] - global_mean_vector[index])
        deltas.append(
            {
                "feature_name": column,
                "value": float(feature_vector[index]),
                "centroid_value": float(centroid_vector[index]),
                "global_mean": float(global_mean_vector[index]),
                "delta_vs_centroid": delta_vs_centroid,
                "delta_vs_global_mean": delta_vs_global,
                "absolute_delta": abs(delta_vs_centroid),
            }
        )
    deltas.sort(key=lambda row: float(row["absolute_delta"]), reverse=True)
    return [{key: value for key, value in row.items() if key != "absolute_delta"} for row in deltas[:limit]]


def confidence_score(*, hands_observed: int, nearest_distance: float, second_distance: float) -> float:
    hands_factor = min(max(hands_observed, 0) / 200.0, 1.0)
    if second_distance <= 0:
        separation = 0.0
    else:
        separation = max(second_distance - nearest_distance, 0.0) / second_distance
    score = (0.6 * hands_factor) + (0.4 * separation)
    return round(float(min(max(score, 0.0), 0.99)), 4)

