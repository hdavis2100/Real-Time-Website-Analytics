from __future__ import annotations

import json

import pandas as pd

from dashboard.config import PREFERRED_METRIC_COLUMNS
from dashboard.data_access import safe_numeric


def _ensure_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = pd.NA
    return result


def _normalize_centroid_metrics(centroids: pd.DataFrame) -> pd.DataFrame:
    if centroids.empty:
        return centroids.copy()

    frame = centroids.copy()
    if "centroid_json" in frame.columns:
        expanded_rows = []
        for value in frame["centroid_json"]:
            if isinstance(value, dict):
                expanded_rows.append(value)
                continue
            if isinstance(value, str) and value.strip():
                try:
                    parsed = json.loads(value)
                except json.JSONDecodeError:
                    parsed = {}
                expanded_rows.append(parsed if isinstance(parsed, dict) else {})
            else:
                expanded_rows.append({})
        expanded = pd.DataFrame(expanded_rows)
        frame = pd.concat([frame.reset_index(drop=True), expanded.reset_index(drop=True)], axis=1)

    alias_map = {
        "vpip": "vpip_rate",
        "pfr": "pfr_rate",
        "cbet_rate": "flop_cbet_rate",
    }
    for source, target in alias_map.items():
        if target not in frame.columns and source in frame.columns:
            frame[target] = frame[source]
    return frame


def summarize_player_clusters(
    training_features: pd.DataFrame,
    assignments: pd.DataFrame | None = None,
    centroids: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if training_features.empty and (assignments is None or assignments.empty):
        return pd.DataFrame(
            columns=[
                "cluster_id",
                "cluster_label",
                "player_count",
                "hands_played",
                "avg_vpip_rate",
                "avg_pfr_rate",
                "avg_aggression_frequency",
                "avg_river_seen_rate",
            ]
        )

    frame = training_features.copy()
    if frame.empty and assignments is not None and not assignments.empty:
        frame = assignments.copy()
    elif assignments is not None and not assignments.empty:
        join_columns = [column for column in ["player_id", "source_dataset"] if column in frame.columns and column in assignments.columns]
        assignment_columns = join_columns + [
            column
            for column in ["cluster_id", "cluster_label", "model_run_id", "distance_to_centroid"]
            if column in assignments.columns
        ]
        if join_columns and assignment_columns:
            frame = frame.merge(
                assignments[assignment_columns].drop_duplicates(join_columns),
                on=join_columns,
                how="left",
                suffixes=("", "_assignment"),
            )

    frame = _ensure_columns(
        frame,
        [
            "player_id",
            "cluster_id",
            "cluster_label",
            "hands_played",
            "vpip_rate",
            "pfr_rate",
            "aggression_frequency",
            "river_seen_rate",
        ],
    )
    frame = safe_numeric(
        frame,
        ["hands_played", "vpip_rate", "pfr_rate", "aggression_frequency", "river_seen_rate"],
    )
    grouped = (
        frame.groupby(["cluster_id", "cluster_label"], dropna=False)
        .agg(
            player_count=("player_id", "nunique"),
            hands_played=("hands_played", "sum"),
            avg_vpip_rate=("vpip_rate", "mean"),
            avg_pfr_rate=("pfr_rate", "mean"),
            avg_aggression_frequency=("aggression_frequency", "mean"),
            avg_river_seen_rate=("river_seen_rate", "mean"),
        )
        .reset_index()
        .sort_values(["hands_played", "player_count"], ascending=False)
        .reset_index(drop=True)
    )

    if centroids is not None and not centroids.empty:
        centroid_frame = _normalize_centroid_metrics(centroids)
        centroid_columns = [column for column in ["cluster_id", "cluster_label", "summary_text", *PREFERRED_METRIC_COLUMNS] if column in centroid_frame.columns]
        if centroid_columns:
            grouped = grouped.merge(
                centroid_frame[centroid_columns].drop_duplicates(["cluster_id"]),
                on=["cluster_id", "cluster_label"],
                how="left",
                suffixes=("", "_centroid"),
            )

    return grouped


def summarize_live_metrics(live_metrics: pd.DataFrame) -> pd.DataFrame:
    if live_metrics.empty:
        return pd.DataFrame(
            columns=[
                "simulation_run_id",
                "agent_count",
                "avg_actions_per_second",
                "avg_hands_per_second",
                "avg_vpip",
                "avg_pfr",
                "avg_aggression_frequency",
                "avg_c_bet_rate",
                "avg_all_in_rate",
                "avg_bb_won",
            ]
        )
    frame = safe_numeric(
        live_metrics.copy(),
        [
            "actions_per_second",
            "hands_per_second",
            "vpip",
            "pfr",
            "aggression_frequency",
            "cbet_rate",
            "all_in_rate",
            "bb_won",
            "observed_hands",
            "observed_actions",
        ],
    )
    group_cols = [col for col in ["simulation_run_id", "run_id"] if col in frame.columns]
    if not group_cols:
        frame["simulation_run_id"] = "unknown"
        group_cols = ["simulation_run_id"]
    summary = (
        frame.groupby(group_cols, dropna=False)
        .agg(
            agent_count=("agent_id", "nunique") if "agent_id" in frame.columns else ("actions_per_second", "size"),
            avg_actions_per_second=("actions_per_second", "mean") if "actions_per_second" in frame.columns else ("bb_won", "size"),
            avg_hands_per_second=("hands_per_second", "mean") if "hands_per_second" in frame.columns else ("bb_won", "size"),
            avg_vpip=("vpip", "mean") if "vpip" in frame.columns else ("bb_won", "size"),
            avg_pfr=("pfr", "mean") if "pfr" in frame.columns else ("bb_won", "size"),
            avg_aggression_frequency=("aggression_frequency", "mean") if "aggression_frequency" in frame.columns else ("bb_won", "size"),
            avg_c_bet_rate=("cbet_rate", "mean") if "cbet_rate" in frame.columns else ("bb_won", "size"),
            avg_all_in_rate=("all_in_rate", "mean") if "all_in_rate" in frame.columns else ("bb_won", "size"),
            avg_bb_won=("bb_won", "mean") if "bb_won" in frame.columns else ("actions_per_second", "size"),
        )
        .reset_index()
    )
    return summary


def filter_actions(
    actions: pd.DataFrame,
    *,
    player_id: str | None = None,
    agent_id: str | None = None,
    street: str | None = None,
    action_type: str | None = None,
    source_type: str | None = None,
    persona_name: str | None = None,
    simulation_run_id: str | None = None,
    table_id: str | None = None,
    limit: int = 200,
) -> pd.DataFrame:
    if actions.empty:
        return actions.copy()
    frame = actions.copy()
    filters = {
        "simulation_run_id": simulation_run_id,
        "player_id": player_id,
        "agent_id": agent_id,
        "street": street,
        "action_type": action_type,
        "source_type": source_type,
        "persona_name": persona_name,
        "table_id": table_id,
    }
    for column, value in filters.items():
        if value and column in frame.columns:
            frame = frame[frame[column].astype(str) == str(value)]
    sort_columns = [col for col in ["event_ts", "timestamp", "hand_id", "action_index"] if col in frame.columns]
    if sort_columns:
        frame = frame.sort_values(sort_columns)
    if limit is not None and len(frame) > limit:
        frame = frame.head(limit)
    return frame.reset_index(drop=True)


def compare_personas_to_clusters(centroids: pd.DataFrame, personas: pd.DataFrame) -> pd.DataFrame:
    if centroids.empty or personas.empty:
        return pd.DataFrame(
            columns=[
                "persona_name",
                "cluster_id",
                "cluster_label",
                "style_distance",
                "vpip_delta",
                "pfr_delta",
                "aggression_delta",
            ]
        )

    metric_pairs = [
        ("vpip", "vpip_rate"),
        ("pfr", "pfr_rate"),
        ("aggression_frequency", "aggression_frequency"),
        ("cbet_rate", "flop_cbet_rate"),
        ("all_in_rate", "all_in_rate"),
    ]
    persona_frame = safe_numeric(personas.copy(), [pair[0] for pair in metric_pairs if pair[0] in personas.columns])
    centroid_frame = safe_numeric(_normalize_centroid_metrics(centroids), [pair[1] for pair in metric_pairs])
    rows: list[dict[str, object]] = []
    for _, persona in persona_frame.iterrows():
        best_match: dict[str, object] | None = None
        for _, centroid in centroid_frame.iterrows():
            deltas = []
            for persona_col, centroid_col in metric_pairs:
                if persona_col in persona and centroid_col in centroid and pd.notna(persona[persona_col]) and pd.notna(centroid[centroid_col]):
                    deltas.append(abs(float(persona[persona_col]) - float(centroid[centroid_col])))
            distance = sum(deltas) / len(deltas) if deltas else float("nan")
            candidate = {
                "persona_name": persona.get("persona_name", "unknown"),
                "persona_text": persona.get("persona_text", ""),
                "cluster_id": centroid.get("cluster_id", ""),
                "cluster_label": centroid.get("cluster_label", ""),
                "style_distance": distance,
                "vpip_delta": float(persona.get("vpip", 0.0)) - float(centroid.get("vpip_rate", 0.0)) if pd.notna(persona.get("vpip")) and pd.notna(centroid.get("vpip_rate")) else pd.NA,
                "pfr_delta": float(persona.get("pfr", 0.0)) - float(centroid.get("pfr_rate", 0.0)) if pd.notna(persona.get("pfr")) and pd.notna(centroid.get("pfr_rate")) else pd.NA,
                "aggression_delta": float(persona.get("aggression_frequency", 0.0)) - float(centroid.get("aggression_frequency", 0.0)) if pd.notna(persona.get("aggression_frequency")) and pd.notna(centroid.get("aggression_frequency")) else pd.NA,
            }
            if best_match is None or (pd.notna(candidate["style_distance"]) and candidate["style_distance"] < best_match["style_distance"]):
                best_match = candidate
        if best_match is not None:
            rows.append(best_match)
    return pd.DataFrame(rows).sort_values(["style_distance", "persona_name"], na_position="last").reset_index(drop=True)


def compare_live_agents_to_clusters(
    live_metrics: pd.DataFrame,
    live_assignments: pd.DataFrame,
    centroids: pd.DataFrame,
    personas: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if live_metrics.empty or live_assignments.empty:
        return pd.DataFrame(
            columns=[
                "simulation_run_id",
                "agent_id",
                "player_id",
                "persona_name",
                "nearest_cluster_label",
                "distance_to_centroid",
                "vpip",
                "pfr",
                "aggression_frequency",
                "cbet_rate",
                "all_in_rate",
                "intent_vpip",
                "intent_pfr",
                "intent_aggression_frequency",
                "vpip_delta_vs_cluster",
                "pfr_delta_vs_cluster",
                "aggression_delta_vs_cluster",
            ]
        )

    metrics = live_metrics.copy()
    sort_columns = [col for col in ["metric_window_end", "window_end_ts", "updated_at"] if col in metrics.columns]
    if sort_columns:
        metrics = metrics.sort_values(sort_columns)
    metrics = metrics.drop_duplicates(["simulation_run_id", "agent_id", "player_id"], keep="last")
    metrics = safe_numeric(metrics, ["vpip", "pfr", "aggression_frequency", "cbet_rate", "all_in_rate"])

    assignments = safe_numeric(live_assignments.copy(), ["distance_to_centroid"])
    merged = metrics.merge(
        assignments[
            [
                col
                for col in [
                    "simulation_run_id",
                    "agent_id",
                    "player_id",
                    "nearest_cluster_id",
                    "nearest_cluster_label",
                    "distance_to_centroid",
                ]
                if col in assignments.columns
            ]
        ],
        on=[col for col in ["simulation_run_id", "agent_id", "player_id"] if col in metrics.columns and col in assignments.columns],
        how="left",
    )

    centroid_metrics = _normalize_centroid_metrics(centroids)
    if not centroid_metrics.empty:
        merged = merged.merge(
            centroid_metrics[
                [
                    col
                    for col in [
                        "cluster_id",
                        "cluster_label",
                        "vpip_rate",
                        "pfr_rate",
                        "aggression_frequency",
                        "flop_cbet_rate",
                        "all_in_rate",
                    ]
                    if col in centroid_metrics.columns
                ]
            ].rename(
                columns={
                    "cluster_id": "nearest_cluster_id",
                    "cluster_label": "historical_cluster_label",
                    "vpip_rate": "cluster_vpip_rate",
                    "pfr_rate": "cluster_pfr_rate",
                    "aggression_frequency": "cluster_aggression_frequency",
                    "flop_cbet_rate": "cluster_cbet_rate",
                    "all_in_rate": "cluster_all_in_rate",
                }
            ),
            on="nearest_cluster_id",
            how="left",
        )

    if personas is not None and not personas.empty:
        persona_columns = [
            col
            for col in [
                "persona_name",
                "vpip",
                "pfr",
                "aggression_frequency",
            ]
            if col in personas.columns
        ]
        if persona_columns:
            merged = merged.merge(
                personas[persona_columns].rename(
                    columns={
                        "vpip": "intent_vpip",
                        "pfr": "intent_pfr",
                        "aggression_frequency": "intent_aggression_frequency",
                    }
                ),
                on="persona_name",
                how="left",
            )

    for column in ["intent_vpip", "intent_pfr", "intent_aggression_frequency"]:
        if column not in merged.columns:
            merged[column] = pd.NA

    merged["vpip_delta_vs_cluster"] = merged.get("vpip") - merged.get("cluster_vpip_rate")
    merged["pfr_delta_vs_cluster"] = merged.get("pfr") - merged.get("cluster_pfr_rate")
    merged["aggression_delta_vs_cluster"] = merged.get("aggression_frequency") - merged.get("cluster_aggression_frequency")

    preferred_columns = [
        "simulation_run_id",
        "agent_id",
        "player_id",
        "persona_name",
        "nearest_cluster_label",
        "historical_cluster_label",
        "distance_to_centroid",
        "vpip",
        "pfr",
        "aggression_frequency",
        "cbet_rate",
        "all_in_rate",
        "intent_vpip",
        "intent_pfr",
        "intent_aggression_frequency",
        "vpip_delta_vs_cluster",
        "pfr_delta_vs_cluster",
        "aggression_delta_vs_cluster",
    ]
    return merged[[column for column in preferred_columns if column in merged.columns]].sort_values(
        [col for col in ["simulation_run_id", "distance_to_centroid", "agent_id"] if col in merged.columns]
    ).reset_index(drop=True)


def latest_rows(frame: pd.DataFrame, sort_columns: list[str], limit: int = 25) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    present = [col for col in sort_columns if col in frame.columns]
    if present:
        frame = frame.sort_values(present)
    if limit is not None and len(frame) > limit:
        frame = frame.tail(limit)
    return frame.reset_index(drop=True)
