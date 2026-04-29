from __future__ import annotations

import argparse
from dataclasses import asdict, is_dataclass
from datetime import datetime
import json
import math
import os
import re
from typing import Any

import pandas as pd

from dashboard.data_access import load_live_dashboard_data
from poker_platform.config import get_config
from poker_platform.storage import bootstrap_backend, get_warehouse

PERFORMANCE_QUERY_DIMENSIONS = [
    "simulation_run_id",
    "decision_backend",
    "hero_context_hash",
    "player_id",
    "agent_id",
    "persona_name",
    "backend_type",
    "final_rank",
    "profile_status",
    "cluster_label",
]

PERFORMANCE_QUERY_SORT_FIELDS = set(
    PERFORMANCE_QUERY_DIMENSIONS
    + [
        "updated_at",
        "hands_played",
        "total_bb_won",
        "bb_per_100",
        "confidence_score",
        "run_count",
        "total_hands",
        "avg_bb_per_100",
        "avg_finish_rank",
        "first_place_rate",
        "ready_profiles",
        "avg_confidence_score",
    ]
)


def _serialize(value: Any) -> Any:
    if is_dataclass(value):
        return _serialize(asdict(value))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): _serialize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, tuple):
        return [_serialize(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    return value


def _print_json(payload: Any) -> None:
    print(json.dumps(_serialize(payload), indent=2, allow_nan=False))


def _warehouse_or_default(warehouse=None):
    if warehouse is not None:
        return warehouse
    if str(os.environ.get("POKER_PLATFORM_SKIP_SCHEMA_BOOTSTRAP", "")).strip() == "1":
        return get_warehouse(get_config())
    return bootstrap_backend()


def _safe_numeric(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    for column in columns:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    return result


def _hero_only_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "is_hero_player" not in frame.columns:
        return frame.copy()
    hero_mask = frame["is_hero_player"].fillna(False).astype(bool)
    if hero_mask.any():
        return frame.loc[hero_mask].copy()
    return frame.copy()


def _normalize_row_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    normalized = frame.where(pd.notna(frame), None)
    return [_serialize(record) for record in normalized.to_dict(orient="records")]


def _deserialize_jsonish(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if normalized and normalized[0] in "[{" and normalized[-1] in "]}":
            try:
                return json.loads(normalized)
            except json.JSONDecodeError:
                return value
    return value


def _normalize_result_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        str(key): _serialize(_deserialize_jsonish(value))
        for key, value in record.items()
        if not (isinstance(value, float) and pd.isna(value))
    }


def _sorted_records(
    frame: pd.DataFrame,
    *,
    sort_columns: list[str],
    ascending: list[bool],
    topn: int = 10,
) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    available_columns = [column for column in sort_columns if column in frame.columns]
    ordered = frame.copy()
    if available_columns:
        available_ascending = [
            ascending[index]
            for index, column in enumerate(sort_columns)
            if column in frame.columns
        ]
        ordered = ordered.sort_values(available_columns, ascending=available_ascending, na_position="last")
    limited = ordered.head(max(1, int(topn)))
    normalized = limited.where(pd.notna(limited), None)
    return [_normalize_result_record(record) for record in normalized.to_dict(orient="records")]


def _player_summary_leaderboard(
    frame: pd.DataFrame,
    *,
    metric: str,
    topn: int = 10,
) -> list[dict[str, Any]]:
    if frame.empty or metric not in frame.columns:
        return []
    ordered = frame.copy()
    numeric_columns = [metric, "total_bb_won", "bb_per_100", "final_rank", "hands_played"]
    ordered = _safe_numeric(ordered, [column for column in numeric_columns if column in ordered.columns])
    return _sorted_records(
        ordered,
        sort_columns=[metric, "total_bb_won", "final_rank", "seat"],
        ascending=[False, False, True, True],
        topn=topn,
    )


def _high_hand_leaderboard(frame: pd.DataFrame, *, topn: int = 10) -> list[dict[str, Any]]:
    if frame.empty or "showdown_hand_score" not in frame.columns:
        return []
    working = frame.copy()
    working = _safe_numeric(working, ["showdown_hand_score"])
    working = working[working["showdown_hand_score"].notna()].copy()
    if working.empty:
        return []

    dedupe_columns = [
        column
        for column in ["simulation_run_id", "player_id", "agent_id"]
        if column in working.columns
    ]
    sort_columns = [column for column in ["showdown_hand_score", "seat"] if column in working.columns]
    ascending = [False if column == "showdown_hand_score" else True for column in sort_columns]
    working = working.sort_values(sort_columns, ascending=ascending, na_position="last")
    if dedupe_columns:
        working = working.drop_duplicates(subset=dedupe_columns, keep="first")
    keep_columns = [
        column
        for column in [
            "simulation_run_id",
            "user_id",
            "decision_backend",
            "hero_context_hash",
            "is_hero_player",
            "player_id",
            "agent_id",
            "seat",
            "player_name",
            "persona_name",
            "showdown_hand_category",
            "showdown_hand_score",
            "made_showdown",
        ]
        if column in working.columns
    ]
    return _sorted_records(
        working[keep_columns],
        sort_columns=["showdown_hand_score", "seat"],
        ascending=[False, True],
        topn=topn,
    )


def _extract_hand_order(value: Any) -> tuple[int, str]:
    text = str(value or "").strip()
    if not text:
        return (10**9, "")
    match = re.search(r"(\d+)(?!.*\d)", text)
    if match:
        return (int(match.group(1)), text)
    return (10**9, text)


def _hero_profit_timeseries(
    player_frame: pd.DataFrame,
    *,
    hand_frame: pd.DataFrame | None = None,
) -> list[dict[str, Any]]:
    if player_frame.empty or "hand_id" not in player_frame.columns:
        return []

    working = player_frame.copy()
    if "is_hero_player" in working.columns and working["is_hero_player"].fillna(False).astype(bool).any():
        working = working[working["is_hero_player"].fillna(False).astype(bool)].copy()
    elif "player_id" in working.columns or "agent_id" in working.columns:
        player_ids = working.get("player_id", pd.Series(dtype="object")).astype(str)
        agent_ids = working.get("agent_id", pd.Series(dtype="object")).astype(str)
        working = working[(player_ids == "user_agent") | (agent_ids == "user_agent")].copy()
    if working.empty:
        return []

    working["result_bb"] = pd.to_numeric(working.get("result_bb"), errors="coerce").fillna(0.0)
    if hand_frame is not None and not hand_frame.empty and "hand_id" in hand_frame.columns:
        hand_order = hand_frame.copy()
        merge_columns = [column for column in ["hand_id", "finished_at", "started_at"] if column in hand_order.columns]
        if merge_columns:
            hand_order = hand_order[merge_columns].drop_duplicates(subset=["hand_id"], keep="last")
            working = working.merge(hand_order, on="hand_id", how="left")

    if "finished_at" in working.columns:
        working["finished_at"] = pd.to_datetime(working["finished_at"], utc=True, errors="coerce")
    if "started_at" in working.columns:
        working["started_at"] = pd.to_datetime(working["started_at"], utc=True, errors="coerce")
    working["hand_sort_order"] = working["hand_id"].map(lambda value: _extract_hand_order(value)[0])
    sort_columns = [column for column in ["finished_at", "started_at", "hand_sort_order", "hand_id"] if column in working.columns]
    ascending = [True] * len(sort_columns)
    if sort_columns:
        working = working.sort_values(sort_columns, ascending=ascending, na_position="last")

    cumulative = 0.0
    points: list[dict[str, Any]] = []
    for index, row in enumerate(working.to_dict(orient="records"), start=1):
        result_bb = float(row.get("result_bb") or 0.0)
        cumulative += result_bb
        points.append(
            {
                "simulation_run_id": row.get("simulation_run_id"),
                "hand_id": row.get("hand_id"),
                "hand_number": index,
                "result_bb": round(result_bb, 4),
                "cumulative_bb_won": round(cumulative, 4),
            }
        )
    return points


def _result_summary(frame: pd.DataFrame) -> dict[str, Any] | None:
    if frame.empty:
        return None
    working = frame.copy()
    working = _safe_numeric(working, ["total_bb_won", "bb_per_100", "final_rank", "hands_played"])
    sort_columns = [column for column in ["final_rank", "total_bb_won", "seat"] if column in working.columns]
    ascending = [
        True if column in {"final_rank", "seat"} else False
        for column in sort_columns
    ]
    ordered = working.sort_values(sort_columns, ascending=ascending, na_position="last")
    winner_record = _normalize_result_record(ordered.iloc[0].to_dict())
    hero_frame = _hero_only_frame(working)
    hero_record = (
        _normalize_result_record(hero_frame.iloc[0].to_dict()) if not hero_frame.empty else None
    )
    return {
        "winner": winner_record,
        "hero": hero_record,
    }


_RUN_METRIC_GROUP_COLUMNS = [
    "simulation_run_id",
    "user_id",
    "decision_backend",
    "hero_context_hash",
    "is_hero_player",
    "agent_id",
    "player_id",
    "persona_name",
    "backend_type",
]
_AGGRESSIVE_ACTIONS = {"bet", "raise"}
_PASSIVE_ACTIONS = {"call"}
_VOLUNTARY_PREFLOP_ACTIONS = {"call", "bet", "raise"}


def _frame_with_columns(frame: pd.DataFrame, columns: list[str], *, default: Any = None) -> pd.DataFrame:
    result = frame.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = default
    return result


def _normalize_metric_group_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    for column in _RUN_METRIC_GROUP_COLUMNS:
        if column not in result.columns:
            result[column] = None if column != "is_hero_player" else False
        if column == "is_hero_player":
            result[column] = result[column].fillna(False).astype(bool)
        else:
            result[column] = result[column].where(pd.notna(result[column]), None).astype("object")
    return result


def _completed_run_metrics(
    action_frame: pd.DataFrame,
    player_detail_frame: pd.DataFrame,
    *,
    topn: int = 25,
) -> list[dict[str, Any]]:
    if action_frame.empty and player_detail_frame.empty:
        return []

    actions = _frame_with_columns(
        action_frame,
        _RUN_METRIC_GROUP_COLUMNS
        + ["hand_id", "street", "action_type", "action_index", "is_all_in", "event_ts"],
    )
    player_details = _frame_with_columns(
        player_detail_frame,
        _RUN_METRIC_GROUP_COLUMNS + ["hand_id", "result_bb"],
    )

    if not actions.empty:
        actions = actions.copy()
        actions["event_ts"] = pd.to_datetime(actions["event_ts"], utc=True, errors="coerce")
        actions["action_index"] = pd.to_numeric(actions["action_index"], errors="coerce")
        actions["is_all_in"] = actions["is_all_in"].fillna(False).astype(bool)
        actions["is_hero_player"] = actions["is_hero_player"].fillna(False).astype(bool)
        actions["street"] = actions["street"].fillna("").astype(str)
        actions["action_type"] = actions["action_type"].fillna("").astype(str)
        actions = _normalize_metric_group_columns(actions)

    if not player_details.empty:
        player_details = player_details.copy()
        player_details["result_bb"] = pd.to_numeric(player_details["result_bb"], errors="coerce").fillna(0.0)
        player_details["is_hero_player"] = player_details["is_hero_player"].fillna(False).astype(bool)
        player_details = _normalize_metric_group_columns(player_details)

    hand_group_columns = _RUN_METRIC_GROUP_COLUMNS + ["hand_id"]
    hand_action_features = pd.DataFrame(columns=hand_group_columns)
    action_counts = pd.DataFrame(columns=_RUN_METRIC_GROUP_COLUMNS + ["observed_actions"])
    timing = pd.DataFrame(columns=_RUN_METRIC_GROUP_COLUMNS + ["metric_window_start", "metric_window_end"])

    if not actions.empty:
        actions["is_preflop"] = actions["street"] == "preflop"
        actions["is_flop"] = actions["street"] == "flop"
        actions["is_aggressive"] = actions["action_type"].isin(_AGGRESSIVE_ACTIONS)
        actions["is_passive"] = actions["action_type"].isin(_PASSIVE_ACTIONS)
        actions["is_vpip_action"] = actions["is_preflop"] & actions["action_type"].isin(
            _VOLUNTARY_PREFLOP_ACTIONS
        )
        actions["is_pfr_action"] = actions["is_preflop"] & actions["action_type"].isin(
            _AGGRESSIVE_ACTIONS
        )

        hand_action_features = (
            actions.groupby(hand_group_columns, dropna=False)
            .agg(
                vpip=("is_vpip_action", "max"),
                pfr=("is_pfr_action", "max"),
                aggression_actions=("is_aggressive", "sum"),
                passive_actions=("is_passive", "sum"),
                all_in=("is_all_in", "max"),
                saw_flop=(
                    "street",
                    lambda series: bool(series.isin(["flop", "turn", "river", "showdown"]).any()),
                ),
            )
            .reset_index()
        )
        hand_action_features = _normalize_metric_group_columns(hand_action_features)

        first_flop_aggressor = (
            actions[actions["is_flop"] & actions["is_aggressive"]]
            .sort_values(["hand_id", "action_index"], ascending=[True, True], na_position="last")
            .drop_duplicates(["hand_id"], keep="first")[["hand_id", "player_id"]]
            .rename(columns={"player_id": "flop_aggressor_player_id"})
        )
        hand_action_features = hand_action_features.merge(
            first_flop_aggressor,
            on="hand_id",
            how="left",
        )
        hand_action_features["is_cbet"] = (
            hand_action_features["pfr"]
            & hand_action_features["saw_flop"]
            & (
                hand_action_features["player_id"]
                == hand_action_features["flop_aggressor_player_id"]
            )
        )
        hand_action_features.drop(columns=["flop_aggressor_player_id"], inplace=True)

        action_counts = (
            actions.groupby(_RUN_METRIC_GROUP_COLUMNS, dropna=False)
            .size()
            .reset_index(name="observed_actions")
        )
        action_counts = _normalize_metric_group_columns(action_counts)
        timing = (
            actions.dropna(subset=["event_ts"])
            .groupby(_RUN_METRIC_GROUP_COLUMNS, dropna=False)
            .agg(
                metric_window_start=("event_ts", "min"),
                metric_window_end=("event_ts", "max"),
            )
            .reset_index()
        )
        timing = _normalize_metric_group_columns(timing)

    result_totals = pd.DataFrame(columns=_RUN_METRIC_GROUP_COLUMNS + ["bb_won"])
    if not player_details.empty:
        result_totals = (
            player_details.groupby(_RUN_METRIC_GROUP_COLUMNS, dropna=False)
            .agg(bb_won=("result_bb", "sum"))
            .reset_index()
        )
        result_totals = _normalize_metric_group_columns(result_totals)

    if not player_details.empty:
        base_hands = player_details[hand_group_columns].drop_duplicates().reset_index(drop=True)
        base_hands = _normalize_metric_group_columns(base_hands)
        combined_hand_features = base_hands.merge(
            hand_action_features,
            on=hand_group_columns,
            how="left",
        )
        for column in [
            "vpip",
            "pfr",
            "aggression_actions",
            "passive_actions",
            "all_in",
            "saw_flop",
            "is_cbet",
        ]:
            if column in combined_hand_features.columns:
                combined_hand_features[column] = combined_hand_features[column].fillna(0)
    else:
        combined_hand_features = hand_action_features.copy()

    if combined_hand_features.empty:
        return []

    grouped = (
        combined_hand_features.groupby(_RUN_METRIC_GROUP_COLUMNS, dropna=False)
        .agg(
            observed_hands=("hand_id", "nunique"),
            vpip=("vpip", "mean"),
            pfr=("pfr", "mean"),
            cbet_rate=("is_cbet", "mean"),
            all_in_rate=("all_in", "mean"),
            aggression_actions=("aggression_actions", "sum"),
            passive_actions=("passive_actions", "sum"),
        )
        .reset_index()
    )
    grouped = grouped.merge(action_counts, on=_RUN_METRIC_GROUP_COLUMNS, how="left")
    grouped = grouped.merge(result_totals, on=_RUN_METRIC_GROUP_COLUMNS, how="left")
    grouped = grouped.merge(timing, on=_RUN_METRIC_GROUP_COLUMNS, how="left")
    grouped["observed_actions"] = grouped["observed_actions"].fillna(0).astype(int)
    grouped["bb_won"] = grouped["bb_won"].fillna(0.0)
    grouped["aggression_frequency"] = grouped.apply(
        lambda row: float(row["aggression_actions"])
        / float(row["aggression_actions"] + row["passive_actions"])
        if (row["aggression_actions"] + row["passive_actions"]) > 0
        else 0.0,
        axis=1,
    )
    grouped.drop(columns=["aggression_actions", "passive_actions"], inplace=True)

    if "metric_window_start" in grouped.columns and "metric_window_end" in grouped.columns:
        duration_seconds = (
            pd.to_datetime(grouped["metric_window_end"], utc=True, errors="coerce")
            - pd.to_datetime(grouped["metric_window_start"], utc=True, errors="coerce")
        ).dt.total_seconds()
        duration_seconds = duration_seconds.where(duration_seconds > 0, 1.0).fillna(1.0)
        grouped["actions_per_second"] = grouped["observed_actions"] / duration_seconds
        grouped["hands_per_second"] = grouped["observed_hands"] / duration_seconds
    else:
        grouped["actions_per_second"] = 0.0
        grouped["hands_per_second"] = 0.0

    grouped = _safe_numeric(
        grouped,
        [
            "bb_won",
            "vpip",
            "pfr",
            "aggression_frequency",
            "cbet_rate",
            "all_in_rate",
            "observed_hands",
            "observed_actions",
            "actions_per_second",
            "hands_per_second",
        ],
    )
    return _sorted_records(
        grouped,
        sort_columns=["bb_won", "vpip", "pfr", "player_id"],
        ascending=[False, False, False, True],
        topn=max(topn, 25),
    )


def _load_player_summary_frame(
    *,
    warehouse,
    simulation_run_ids: list[str] | None = None,
    user_id: str | None = None,
    decision_backend: str | None = None,
) -> pd.DataFrame:
    filters_eq: dict[str, Any] = {}
    filters_in: dict[str, list[str]] = {}
    if user_id:
        filters_eq["user_id"] = user_id
    if decision_backend:
        filters_eq["decision_backend"] = decision_backend
    if simulation_run_ids:
        filters_in["simulation_run_id"] = [str(run_id) for run_id in simulation_run_ids if str(run_id).strip()]
    return warehouse.load_table(
        "SIMULATION_RUN_PLAYER_SUMMARIES",
        filters_eq=filters_eq or None,
        filters_in=filters_in or None,
        order_by=[("updated_at", False), ("simulation_run_id", False)],
    )


def _summarize_profile_status(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty or "status" not in frame.columns:
        return {
            "ready_profiles": 0,
            "insufficient_evidence": 0,
            "not_found": 0,
            "other": 0,
        }

    statuses = frame["status"].fillna("unknown").astype(str).str.strip().str.lower()
    counts = statuses.value_counts().to_dict()
    return {
        "ready_profiles": int(counts.get("ready", 0)),
        "insufficient_evidence": int(counts.get("insufficient_evidence", 0)),
        "not_found": int(counts.get("not_found", 0)),
        "other": int(
            sum(
                count
                for status, count in counts.items()
                if status not in {"ready", "insufficient_evidence", "not_found"}
            )
        ),
    }


def _normalize_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_string_list(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = _normalize_string(value)
        if token is None or token in seen:
            continue
        normalized.append(token)
        seen.add(token)
    return normalized


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_query_sort(value: Any) -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    field = _normalize_string(value.get("field"))
    if field not in PERFORMANCE_QUERY_SORT_FIELDS:
        return None
    direction = str(value.get("direction") or "desc").strip().lower()
    return {
        "field": field,
        "direction": "asc" if direction == "asc" else "desc",
    }


def _normalize_performance_query_definition(
    *,
    query: dict[str, Any] | None = None,
    simulation_run_ids: list[str] | None = None,
    user_id: str | None = None,
    decision_backend: str | None = None,
    min_hands_played: int | None = None,
    max_final_rank: int | None = None,
    topn: int = 25,
    group_by: list[str] | None = None,
    sort: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = query if isinstance(query, dict) else {}
    raw_filters = payload.get("filters") if isinstance(payload.get("filters"), dict) else {}
    resolved_group_by = _normalize_string_list(
        payload.get("group_by") if payload.get("group_by") is not None else group_by
    )
    resolved_group_by = [
        dimension
        for dimension in resolved_group_by
        if dimension in PERFORMANCE_QUERY_DIMENSIONS
    ]
    try:
        requested_topn = int(payload.get("topn") or topn or 25)
    except (TypeError, ValueError):
        requested_topn = int(topn or 25)
    resolved_topn = max(
        1,
        min(
            requested_topn,
            100,
        ),
    )
    resolved_decision_backend = _normalize_string(
        raw_filters.get("decision_backend")
        if raw_filters.get("decision_backend") is not None
        else payload.get("decision_backend")
    ) or _normalize_string(decision_backend)

    filters = {
        "simulation_run_ids": _normalize_string_list(
            raw_filters.get("simulation_run_ids")
            if raw_filters.get("simulation_run_ids") is not None
            else payload.get("simulation_run_ids")
            if payload.get("simulation_run_ids") is not None
            else simulation_run_ids
        ),
        "hero_context_hashes": _normalize_string_list(raw_filters.get("hero_context_hashes")),
        "player_ids": _normalize_string_list(raw_filters.get("player_ids")),
        "agent_ids": _normalize_string_list(raw_filters.get("agent_ids")),
        "persona_names": _normalize_string_list(raw_filters.get("persona_names")),
        "profile_statuses": [
            token.lower()
            for token in _normalize_string_list(raw_filters.get("profile_statuses"))
        ],
        "cluster_labels": _normalize_string_list(raw_filters.get("cluster_labels")),
        "decision_backend": resolved_decision_backend,
        "min_hands_played": _optional_int(
            raw_filters.get("min_hands_played")
            if raw_filters.get("min_hands_played") is not None
            else min_hands_played
        ),
        "max_final_rank": _optional_int(
            raw_filters.get("max_final_rank")
            if raw_filters.get("max_final_rank") is not None
            else max_final_rank
        ),
        "min_total_bb_won": _optional_float(raw_filters.get("min_total_bb_won")),
        "max_total_bb_won": _optional_float(raw_filters.get("max_total_bb_won")),
        "min_bb_per_100": _optional_float(raw_filters.get("min_bb_per_100")),
        "max_bb_per_100": _optional_float(raw_filters.get("max_bb_per_100")),
        "date_from": _normalize_string(raw_filters.get("date_from")),
        "date_to": _normalize_string(raw_filters.get("date_to")),
        "status": _normalize_string(raw_filters.get("status")),
        "hero_agent_definition_id": _normalize_string(
            raw_filters.get("hero_agent_definition_id")
        ),
        "hero_agent_version_id": _normalize_string(raw_filters.get("hero_agent_version_id")),
    }
    return {
        "simulation_run_ids": filters["simulation_run_ids"],
        "user_id": _normalize_string(payload.get("user_id")) or _normalize_string(user_id),
        "decision_backend": resolved_decision_backend,
        "topn": resolved_topn,
        "group_by": resolved_group_by,
        "sort": _normalize_query_sort(payload.get("sort") if "sort" in payload else sort),
        "filters": filters,
    }


def _load_profile_result_frame(
    *,
    warehouse,
    simulation_run_ids: list[str],
) -> pd.DataFrame:
    if not simulation_run_ids:
        return pd.DataFrame()
    profile_frame = warehouse.load_table(
        "PROFILE_SESSION_RESULTS",
        filters_in={"simulation_run_id": simulation_run_ids},
        order_by=[("scored_at", False), ("simulation_run_id", False)],
    )
    if profile_frame.empty:
        return profile_frame
    normalized = profile_frame.copy()
    if "status" in normalized.columns:
        normalized["status"] = (
            normalized["status"].fillna("unknown").astype(str).str.strip().str.lower()
        )
    return _safe_numeric(normalized, ["hands_observed", "confidence_score", "cluster_id"])


def _dedupe_latest_records(frame: pd.DataFrame, *, subset: list[str]) -> pd.DataFrame:
    if frame.empty or not all(column in frame.columns for column in subset):
        return pd.DataFrame()
    working = frame.copy()
    sort_columns = [
        column
        for column in ["profile_scored_at", "simulation_run_id", "player_id", "agent_id"]
        if column in working.columns
    ]
    if sort_columns:
        working = working.sort_values(
            sort_columns,
            ascending=[False if column == "profile_scored_at" else True for column in sort_columns],
            na_position="last",
        )
    return working.drop_duplicates(subset=subset, keep="first")


def _enrich_summary_frame_with_profiles(
    summary_frame: pd.DataFrame,
    profile_frame: pd.DataFrame,
) -> pd.DataFrame:
    enriched = summary_frame.copy()
    profile_value_columns = [
        "profile_status",
        "cluster_label",
        "confidence_score",
        "hands_observed",
        "model_run_id",
        "profile_scored_at",
    ]
    for column in profile_value_columns:
        if column not in enriched.columns:
            enriched[column] = None
    if profile_frame.empty:
        return enriched

    profile_columns = [
        column
        for column in [
            "simulation_run_id",
            "player_id",
            "agent_id",
            "status",
            "cluster_label",
            "confidence_score",
            "hands_observed",
            "model_run_id",
            "scored_at",
        ]
        if column in profile_frame.columns
    ]
    normalized_profiles = profile_frame[profile_columns].copy()
    normalized_profiles = normalized_profiles.rename(
        columns={"status": "profile_status", "scored_at": "profile_scored_at"}
    )

    join_specs = [
        ["simulation_run_id", "player_id"],
        ["simulation_run_id", "agent_id"],
        ["simulation_run_id"],
    ]
    for join_columns in join_specs:
        if not all(column in enriched.columns for column in join_columns):
            continue
        deduped = _dedupe_latest_records(normalized_profiles, subset=join_columns)
        if deduped.empty:
            continue
        available_profile_values = [
            column for column in profile_value_columns if column in deduped.columns
        ]
        merged = enriched[join_columns].merge(
            deduped[join_columns + available_profile_values],
            on=join_columns,
            how="left",
        )
        for column in available_profile_values:
            if column in merged.columns:
                enriched[column] = merged[column].where(pd.notna(merged[column]), enriched[column])
    return enriched


def _apply_summary_filters(frame: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    filtered = frame.copy()

    list_filters = [
        ("simulation_run_ids", "simulation_run_id"),
        ("hero_context_hashes", "hero_context_hash"),
        ("player_ids", "player_id"),
        ("agent_ids", "agent_id"),
        ("persona_names", "persona_name"),
        ("cluster_labels", "cluster_label"),
    ]
    for filter_name, column in list_filters:
        values = _normalize_string_list(filters.get(filter_name))
        if values and column in filtered.columns:
            normalized_series = filtered[column].fillna("").astype(str).str.strip()
            filtered = filtered.loc[normalized_series.isin(values)].copy()

    profile_statuses = [
        token.lower() for token in _normalize_string_list(filters.get("profile_statuses"))
    ]
    if profile_statuses and "profile_status" in filtered.columns:
        statuses = filtered["profile_status"].fillna("").astype(str).str.strip().str.lower()
        filtered = filtered.loc[statuses.isin(profile_statuses)].copy()

    if filters.get("decision_backend") and "decision_backend" in filtered.columns:
        filtered = filtered.loc[
            filtered["decision_backend"].fillna("").astype(str).str.strip()
            == str(filters["decision_backend"]).strip()
        ].copy()

    numeric_filters = [
        ("min_hands_played", "hands_played", ">="),
        ("max_final_rank", "final_rank", "<="),
        ("min_total_bb_won", "total_bb_won", ">="),
        ("max_total_bb_won", "total_bb_won", "<="),
        ("min_bb_per_100", "bb_per_100", ">="),
        ("max_bb_per_100", "bb_per_100", "<="),
    ]
    for filter_name, column, operator in numeric_filters:
        value = filters.get(filter_name)
        if value is None or column not in filtered.columns:
            continue
        numeric = pd.to_numeric(filtered[column], errors="coerce")
        if operator == ">=":
            filtered = filtered.loc[numeric >= float(value)].copy()
        else:
            filtered = filtered.loc[numeric <= float(value)].copy()

    return filtered


def _sort_frame(
    frame: pd.DataFrame,
    *,
    sort: dict[str, str] | None = None,
    fallback_columns: list[str] | None = None,
    fallback_ascending: list[bool] | None = None,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    sort_columns: list[str] = []
    ascending: list[bool] = []

    if sort and sort.get("field") in frame.columns:
        sort_columns.append(str(sort["field"]))
        ascending.append(str(sort.get("direction") or "desc").lower() == "asc")

    for index, column in enumerate(fallback_columns or []):
        if column not in frame.columns or column in sort_columns:
            continue
        sort_columns.append(column)
        if fallback_ascending and index < len(fallback_ascending):
            ascending.append(bool(fallback_ascending[index]))
        else:
            ascending.append(True)

    if not sort_columns:
        return frame.copy()
    return frame.sort_values(sort_columns, ascending=ascending, na_position="last")


def _aggregate_query_groups(frame: pd.DataFrame, *, group_by: list[str]) -> pd.DataFrame:
    available = [column for column in group_by if column in frame.columns]
    if frame.empty or not available:
        return pd.DataFrame()

    aggregate_fields: dict[str, tuple[str, str]] = {}
    if "simulation_run_id" in frame.columns:
        aggregate_fields["run_count"] = ("simulation_run_id", "nunique")
    if "hands_played" in frame.columns:
        aggregate_fields["total_hands"] = ("hands_played", "sum")
    if "total_bb_won" in frame.columns:
        aggregate_fields["total_bb_won"] = ("total_bb_won", "sum")
    if "bb_per_100" in frame.columns:
        aggregate_fields["avg_bb_per_100"] = ("bb_per_100", "mean")
    if "final_rank" in frame.columns:
        aggregate_fields["avg_finish_rank"] = ("final_rank", "mean")
    if "confidence_score" in frame.columns:
        aggregate_fields["avg_confidence_score"] = ("confidence_score", "mean")

    if aggregate_fields:
        grouped = frame.groupby(available, dropna=False).agg(**aggregate_fields).reset_index()
    else:
        grouped = frame.groupby(available, dropna=False).size().reset_index(name="row_count")

    if "final_rank" in frame.columns:
        first_place_rate = (
            frame.groupby(available, dropna=False)["final_rank"]
            .apply(
                lambda series: float((series.fillna(999999) == 1).mean())
                if len(series.index)
                else None
            )
            .reset_index(name="first_place_rate")
        )
        grouped = grouped.merge(first_place_rate, on=available, how="left")

    if "profile_status" in frame.columns:
        ready_profiles = (
            frame.groupby(available, dropna=False)["profile_status"]
            .apply(
                lambda series: int(
                    (
                        series.fillna("").astype(str).str.strip().str.lower() == "ready"
                    ).sum()
                )
            )
            .reset_index(name="ready_profiles")
        )
        grouped = grouped.merge(ready_profiles, on=available, how="left")

    return grouped


def load_performance_summary(
    *,
    simulation_run_ids: list[str] | None = None,
    user_id: str | None = None,
    decision_backend: str | None = None,
    topn: int = 10,
    warehouse=None,
) -> dict[str, Any]:
    resolved_run_ids = [
        str(run_id).strip()
        for run_id in (simulation_run_ids or [])
        if str(run_id).strip()
    ]
    warehouse = _warehouse_or_default(warehouse)
    summary_frame = _load_player_summary_frame(
        warehouse=warehouse,
        simulation_run_ids=resolved_run_ids or None,
        user_id=user_id,
        decision_backend=decision_backend,
    )
    summary_frame = _hero_only_frame(summary_frame)
    summary_frame = _safe_numeric(
        summary_frame,
        ["hands_played", "total_bb_won", "bb_per_100", "final_rank"],
    )

    recent_runs: list[dict[str, Any]] = []
    summary: dict[str, Any] | None = None
    if not summary_frame.empty:
        run_count = int(summary_frame["simulation_run_id"].nunique()) if "simulation_run_id" in summary_frame.columns else int(len(summary_frame))
        total_hands = int(summary_frame["hands_played"].fillna(0).sum()) if "hands_played" in summary_frame.columns else 0
        total_bb_won = float(summary_frame["total_bb_won"].fillna(0).sum()) if "total_bb_won" in summary_frame.columns else 0.0
        avg_bb_per_100 = (
            float(summary_frame["bb_per_100"].dropna().mean())
            if "bb_per_100" in summary_frame.columns and summary_frame["bb_per_100"].notna().any()
            else None
        )
        avg_finish_rank = (
            float(summary_frame["final_rank"].dropna().mean())
            if "final_rank" in summary_frame.columns and summary_frame["final_rank"].notna().any()
            else None
        )
        first_place_rate = (
            float((summary_frame["final_rank"] == 1).mean())
            if "final_rank" in summary_frame.columns and len(summary_frame.index) > 0
            else None
        )
        summary = {
            "run_count": run_count,
            "total_hands": total_hands,
            "total_bb_won": total_bb_won,
            "avg_bb_per_100": avg_bb_per_100,
            "avg_finish_rank": avg_finish_rank,
            "first_place_rate": first_place_rate,
        }

        recent_columns = [
            column
            for column in [
                "simulation_run_id",
                "decision_backend",
                "total_bb_won",
                "bb_per_100",
                "final_rank",
                "hands_played",
                "updated_at",
            ]
            if column in summary_frame.columns
        ]
        recent_frame = summary_frame.copy()
        if "updated_at" in recent_frame.columns:
            recent_frame = recent_frame.sort_values(["updated_at", "simulation_run_id"], ascending=[False, False])
        elif "simulation_run_id" in recent_frame.columns:
            recent_frame = recent_frame.sort_values(["simulation_run_id"], ascending=[False])
        recent_runs = _normalize_row_records(recent_frame[recent_columns].head(max(1, int(topn))))

    profile_frame = pd.DataFrame()
    if resolved_run_ids:
        profile_frame = _load_profile_result_frame(
            warehouse=warehouse,
            simulation_run_ids=resolved_run_ids,
        )
        if decision_backend and not profile_frame.empty and "simulation_run_id" in summary_frame.columns:
            allowed_ids = set(summary_frame["simulation_run_id"].dropna().astype(str))
            profile_frame = profile_frame[
                profile_frame["simulation_run_id"].astype(str).isin(allowed_ids)
            ].copy()

    return {
        "summary": _serialize(summary),
        "recent_runs": recent_runs,
        "profile_overview": _summarize_profile_status(profile_frame),
        "filters": {
            "user_id": user_id,
            "decision_backend": decision_backend,
            "simulation_run_ids": resolved_run_ids,
        },
    }


def _simulation_result_scope_filters(
    *,
    simulation_run_id: str,
    run_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    filters: dict[str, Any] = {"simulation_run_id": simulation_run_id}
    summary = run_summary or {}
    for key in ["user_id", "decision_backend", "hero_context_hash"]:
        value = summary.get(key)
        if value not in (None, ""):
            filters[key] = value
    return filters


def load_simulation_results(
    *,
    simulation_run_id: str,
    topn: int = 10,
    warehouse=None,
) -> dict[str, Any]:
    warehouse = _warehouse_or_default(warehouse)
    run_frame = warehouse.load_table(
        "RAW_SIMULATION_RUNS",
        filters_eq={"simulation_run_id": simulation_run_id},
        order_by=[("requested_at", False), ("finished_at", False)],
        limit=1,
    )
    run_summary = (
        _normalize_result_record(run_frame.iloc[0].to_dict()) if not run_frame.empty else {}
    )
    result_scope_filters = _simulation_result_scope_filters(
        simulation_run_id=simulation_run_id,
        run_summary=run_summary,
    )
    player_summary_frame = warehouse.load_table(
        "SIMULATION_RUN_PLAYER_SUMMARIES",
        filters_eq=result_scope_filters,
        order_by=[("final_rank", True), ("seat", True)],
    )
    player_detail_frame = warehouse.load_table(
        "CURATED_PLAYERS",
        filters_eq=result_scope_filters,
        order_by=[("showdown_hand_score", False), ("seat", True)],
    )
    hand_frame = warehouse.load_table(
        "CURATED_HANDS",
        filters_eq=result_scope_filters,
        order_by=[("finished_at", True), ("started_at", True), ("hand_id", True)],
    )
    action_frame = warehouse.load_table(
        "CURATED_ACTIONS",
        filters_eq=result_scope_filters,
        order_by=[("event_ts", True), ("action_index", True)],
    )
    profile_assignment_frame = warehouse.load_table(
        "LIVE_PROFILE_ASSIGNMENTS",
        filters_eq=result_scope_filters,
        order_by=[("assigned_at", False), ("agent_id", True)],
    )

    if run_frame.empty and player_summary_frame.empty and player_detail_frame.empty:
        return {
            "status": "not_found",
            "simulation_run_id": simulation_run_id,
            "message": "No completed results are available yet",
        }

    if player_summary_frame.empty:
        return {
            "status": "pending",
            "source": "snowflake",
            "simulation_run_id": simulation_run_id,
            "message": "Final results are still being prepared",
            "ready": False,
            "run_summary": run_summary,
            "result": {
                "performance": [],
                "summary": None,
            },
            "player_summaries": [],
            "run_profit_leaderboard": [],
            "run_bb_per_100_leaderboard": [],
            "run_high_hand_leaderboard": [],
            "agent_metrics": [],
            "profile_assignments": [],
        }

    player_summaries = _normalize_row_records(player_summary_frame)
    run_profit_leaderboard = _player_summary_leaderboard(
        player_summary_frame,
        metric="total_bb_won",
        topn=topn,
    )
    run_bb_per_100_leaderboard = _player_summary_leaderboard(
        player_summary_frame,
        metric="bb_per_100",
        topn=topn,
    )
    run_high_hand_leaderboard = _high_hand_leaderboard(
        player_detail_frame,
        topn=topn,
    )
    agent_metrics = _completed_run_metrics(
        action_frame,
        player_detail_frame,
        topn=max(topn, 25),
    )
    profile_assignments = _sorted_records(
        profile_assignment_frame,
        sort_columns=["assigned_at", "agent_id"],
        ascending=[False, True],
        topn=max(topn, 25),
    )
    hero_profit_timeseries = _hero_profit_timeseries(
        player_detail_frame,
        hand_frame=hand_frame,
    )

    return {
        "status": "ready",
        "source": "snowflake",
        "simulation_run_id": simulation_run_id,
        "run_summary": run_summary,
        "result": {
            "performance": player_summaries,
            "summary": _result_summary(player_summary_frame),
        },
        "player_summaries": player_summaries,
        "run_profit_leaderboard": run_profit_leaderboard,
        "run_bb_per_100_leaderboard": run_bb_per_100_leaderboard,
        "run_high_hand_leaderboard": run_high_hand_leaderboard,
        "agent_metrics": agent_metrics,
        "profile_assignments": profile_assignments,
        "hero_profit_timeseries": hero_profit_timeseries,
    }


def load_simulation_result_status(
    *,
    simulation_run_id: str,
    warehouse=None,
) -> dict[str, Any]:
    warehouse = _warehouse_or_default(warehouse)
    run_frame = warehouse.load_table(
        "RAW_SIMULATION_RUNS",
        filters_eq={"simulation_run_id": simulation_run_id},
        order_by=[("requested_at", False), ("finished_at", False)],
        limit=1,
    )
    run_summary = (
        _normalize_result_record(run_frame.iloc[0].to_dict()) if not run_frame.empty else {}
    )
    result_scope_filters = _simulation_result_scope_filters(
        simulation_run_id=simulation_run_id,
        run_summary=run_summary,
    )
    player_summary_frame = warehouse.load_table(
        "SIMULATION_RUN_PLAYER_SUMMARIES",
        filters_eq=result_scope_filters,
        order_by=[("final_rank", True), ("seat", True)],
        limit=1,
    )

    if run_frame.empty and player_summary_frame.empty:
        return {
            "status": "pending",
            "source": "snowflake",
            "simulation_run_id": simulation_run_id,
            "message": "Final results are not available yet",
            "ready": False,
            "run_summary": {},
            "has_player_summaries": False,
        }

    has_player_summaries = not player_summary_frame.empty
    return {
        "status": "ready" if has_player_summaries else "pending",
        "source": "snowflake",
        "simulation_run_id": simulation_run_id,
        "message": None if has_player_summaries else "Final results are still being prepared",
        "ready": has_player_summaries,
        "run_summary": run_summary,
        "has_player_summaries": has_player_summaries,
    }


def load_profile_source_status(
    *,
    simulation_run_id: str,
    warehouse=None,
) -> dict[str, Any]:
    warehouse = _warehouse_or_default(warehouse)
    run_frame = warehouse.load_table(
        "RAW_SIMULATION_RUNS",
        filters_eq={"simulation_run_id": simulation_run_id},
        order_by=[("requested_at", False), ("finished_at", False)],
        limit=1,
    )
    run_summary = (
        _normalize_result_record(run_frame.iloc[0].to_dict()) if not run_frame.empty else {}
    )
    result_scope_filters = _simulation_result_scope_filters(
        simulation_run_id=simulation_run_id,
        run_summary=run_summary,
    )
    action_frame = warehouse.load_table(
        "CURATED_ACTIONS",
        filters_eq=result_scope_filters,
        order_by=[("event_ts", True), ("action_index", True)],
        limit=1,
    )
    player_frame = warehouse.load_table(
        "CURATED_PLAYERS",
        filters_eq=result_scope_filters,
        order_by=[("hand_id", True), ("seat", True)],
        limit=1,
    )

    if run_frame.empty and action_frame.empty and player_frame.empty:
        return {
            "status": "pending",
            "source": "snowflake",
            "simulation_run_id": simulation_run_id,
            "message": "Profile inputs are not available yet",
            "ready": False,
            "run_summary": {},
            "has_actions": False,
            "has_players": False,
        }

    has_actions = not action_frame.empty
    has_players = not player_frame.empty
    ready = has_actions and has_players
    return {
        "status": "ready" if ready else "pending",
        "source": "snowflake",
        "simulation_run_id": simulation_run_id,
        "message": None if ready else "Profile inputs are still being prepared",
        "ready": ready,
        "run_summary": run_summary,
        "has_actions": has_actions,
        "has_players": has_players,
    }


def load_performance_query(
    *,
    query: dict[str, Any] | None = None,
    simulation_run_ids: list[str] | None = None,
    user_id: str | None = None,
    decision_backend: str | None = None,
    min_hands_played: int | None = None,
    max_final_rank: int | None = None,
    topn: int = 25,
    group_by: list[str] | None = None,
    sort: dict[str, Any] | None = None,
    warehouse=None,
) -> dict[str, Any]:
    query_definition = _normalize_performance_query_definition(
        query=query,
        simulation_run_ids=simulation_run_ids,
        user_id=user_id,
        decision_backend=decision_backend,
        min_hands_played=min_hands_played,
        max_final_rank=max_final_rank,
        topn=topn,
        group_by=group_by,
        sort=sort,
    )
    resolved_run_ids = query_definition["simulation_run_ids"]
    warehouse = _warehouse_or_default(warehouse)
    summary_frame = _load_player_summary_frame(
        warehouse=warehouse,
        simulation_run_ids=resolved_run_ids or None,
        user_id=query_definition["user_id"],
        decision_backend=query_definition["decision_backend"],
    )
    summary_frame = _hero_only_frame(summary_frame)
    summary_frame = _safe_numeric(
        summary_frame,
        ["hands_played", "total_bb_won", "bb_per_100", "final_rank", "confidence_score"],
    )
    candidate_profile_run_ids = resolved_run_ids
    if not candidate_profile_run_ids and "simulation_run_id" in summary_frame.columns:
        candidate_profile_run_ids = (
            summary_frame["simulation_run_id"].dropna().astype(str).drop_duplicates().tolist()
        )
    profile_frame = _load_profile_result_frame(
        warehouse=warehouse,
        simulation_run_ids=candidate_profile_run_ids,
    )
    summary_frame = _enrich_summary_frame_with_profiles(summary_frame, profile_frame)
    summary_frame = _safe_numeric(summary_frame, ["confidence_score", "hands_observed"])
    summary_frame = _apply_summary_filters(summary_frame, query_definition["filters"])

    summary: dict[str, Any] | None = None
    rows: list[dict[str, Any]] = []
    comparison_rows: list[dict[str, Any]] = []
    breakdown_by_backend: list[dict[str, Any]] = []
    breakdown_by_finish_rank: list[dict[str, Any]] = []
    breakdown_by_profile_status: list[dict[str, Any]] = []
    breakdown_by_cluster_label: list[dict[str, Any]] = []
    matched_run_ids: list[str] = []

    if not summary_frame.empty:
        matched_run_ids = (
            summary_frame["simulation_run_id"].dropna().astype(str).drop_duplicates().tolist()
            if "simulation_run_id" in summary_frame.columns
            else []
        )
        summary = {
            "run_count": int(len(matched_run_ids)),
            "total_hands": int(summary_frame["hands_played"].fillna(0).sum()) if "hands_played" in summary_frame.columns else 0,
            "total_bb_won": float(summary_frame["total_bb_won"].fillna(0).sum()) if "total_bb_won" in summary_frame.columns else 0.0,
            "avg_bb_per_100": (
                float(summary_frame["bb_per_100"].dropna().mean())
                if "bb_per_100" in summary_frame.columns and summary_frame["bb_per_100"].notna().any()
                else None
            ),
            "avg_finish_rank": (
                float(summary_frame["final_rank"].dropna().mean())
                if "final_rank" in summary_frame.columns and summary_frame["final_rank"].notna().any()
                else None
            ),
            "first_place_rate": (
                float((summary_frame["final_rank"] == 1).mean())
                if "final_rank" in summary_frame.columns and len(summary_frame.index) > 0
                else None
            ),
        }
        keep_columns = [
            column
            for column in [
                "simulation_run_id",
                "decision_backend",
                "hero_context_hash",
                "player_id",
                "agent_id",
                "persona_name",
                "hands_played",
                "total_bb_won",
                "bb_per_100",
                "final_rank",
                "profile_status",
                "cluster_label",
                "confidence_score",
                "hands_observed",
                "updated_at",
            ]
            if column in summary_frame.columns
        ]
        detail_frame = _sort_frame(
            summary_frame[keep_columns],
            sort=query_definition["sort"],
            fallback_columns=["total_bb_won", "bb_per_100", "final_rank", "updated_at"],
            fallback_ascending=[False, False, True, False],
        )
        rows = _normalize_row_records(detail_frame.head(query_definition["topn"]))

        if "decision_backend" in summary_frame.columns:
            backend_frame = _aggregate_query_groups(
                summary_frame,
                group_by=["decision_backend"],
            )
            backend_frame = _sort_frame(
                backend_frame,
                fallback_columns=["total_bb_won", "avg_bb_per_100", "decision_backend"],
                fallback_ascending=[False, False, True],
            )
            breakdown_by_backend = _normalize_row_records(
                backend_frame.head(min(query_definition["topn"], 25))
            )

        if "final_rank" in summary_frame.columns:
            finish_frame = _aggregate_query_groups(
                summary_frame,
                group_by=["final_rank"],
            )
            finish_frame = _sort_frame(
                finish_frame,
                fallback_columns=["final_rank"],
                fallback_ascending=[True],
            )
            breakdown_by_finish_rank = _normalize_row_records(
                finish_frame.head(min(query_definition["topn"], 25))
            )

        if "profile_status" in summary_frame.columns and summary_frame["profile_status"].notna().any():
            profile_status_frame = _aggregate_query_groups(
                summary_frame,
                group_by=["profile_status"],
            )
            profile_status_frame = _sort_frame(
                profile_status_frame,
                fallback_columns=["run_count", "profile_status"],
                fallback_ascending=[False, True],
            )
            breakdown_by_profile_status = _normalize_row_records(
                profile_status_frame.head(min(query_definition["topn"], 25))
            )

        if "cluster_label" in summary_frame.columns and summary_frame["cluster_label"].notna().any():
            cluster_frame = summary_frame[
                summary_frame["cluster_label"].notna()
            ].copy()
            cluster_frame = _aggregate_query_groups(
                cluster_frame,
                group_by=["cluster_label"],
            )
            cluster_frame = _sort_frame(
                cluster_frame,
                fallback_columns=["total_bb_won", "cluster_label"],
                fallback_ascending=[False, True],
            )
            breakdown_by_cluster_label = _normalize_row_records(
                cluster_frame.head(min(query_definition["topn"], 25))
            )

        comparison_frame = _aggregate_query_groups(
            summary_frame,
            group_by=query_definition["group_by"],
        )
        if not comparison_frame.empty:
            comparison_frame = _sort_frame(
                comparison_frame,
                sort=query_definition["sort"],
                fallback_columns=["total_bb_won", "avg_bb_per_100"],
                fallback_ascending=[False, False],
            )
            comparison_rows = _normalize_row_records(
                comparison_frame.head(query_definition["topn"])
            )

    matched_profile_frame = profile_frame
    if matched_run_ids and not profile_frame.empty and "simulation_run_id" in profile_frame.columns:
        matched_profile_frame = profile_frame[
            profile_frame["simulation_run_id"].astype(str).isin(set(matched_run_ids))
        ].copy()

    return {
        "status": "ready",
        "summary": _serialize(summary),
        "rows": rows,
        "comparison_rows": comparison_rows,
        "recent_runs": rows,
        "matched_run_ids": matched_run_ids,
        "matched_run_count": len(matched_run_ids),
        "profile_overview": _summarize_profile_status(matched_profile_frame),
        "breakdowns": {
            "by_backend": breakdown_by_backend,
            "by_finish_rank": breakdown_by_finish_rank,
            "by_profile_status": breakdown_by_profile_status,
            "by_cluster_label": breakdown_by_cluster_label,
        },
        "query": {
            "user_id": query_definition["user_id"],
            "decision_backend": query_definition["decision_backend"],
            "simulation_run_ids": resolved_run_ids,
            "group_by": query_definition["group_by"],
            "sort": query_definition["sort"],
            "topn": query_definition["topn"],
            "filters": _serialize(query_definition["filters"]),
        },
        "filters": _serialize(query_definition["filters"]),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bridge dashboard data to JSON for app routes.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    live_parser = subparsers.add_parser("live")
    live_parser.add_argument("--simulation-run-id")
    live_parser.add_argument("--user-id")
    live_parser.add_argument("--decision-backend")
    live_parser.add_argument("--topn", type=int, default=25)

    performance_parser = subparsers.add_parser("performance-summary")
    performance_parser.add_argument("--simulation-run-id", action="append", default=[])
    performance_parser.add_argument("--user-id")
    performance_parser.add_argument("--decision-backend")
    performance_parser.add_argument("--topn", type=int, default=10)

    simulation_results_parser = subparsers.add_parser("simulation-results")
    simulation_results_parser.add_argument("--simulation-run-id", required=True)
    simulation_results_parser.add_argument("--topn", type=int, default=10)

    simulation_result_status_parser = subparsers.add_parser("simulation-results-status")
    simulation_result_status_parser.add_argument("--simulation-run-id", required=True)
    profile_source_status_parser = subparsers.add_parser("profile-source-status")
    profile_source_status_parser.add_argument("--simulation-run-id", required=True)

    performance_query_parser = subparsers.add_parser("performance-query")
    performance_query_parser.add_argument("--simulation-run-id", action="append", default=[])
    performance_query_parser.add_argument("--user-id")
    performance_query_parser.add_argument("--decision-backend")
    performance_query_parser.add_argument("--min-hands-played", type=int)
    performance_query_parser.add_argument("--max-final-rank", type=int)
    performance_query_parser.add_argument("--group-by", action="append", default=[])
    performance_query_parser.add_argument("--sort-field")
    performance_query_parser.add_argument("--sort-direction")
    performance_query_parser.add_argument("--query-json")
    performance_query_parser.add_argument("--topn", type=int, default=25)

    args = parser.parse_args(argv)

    if args.command == "live":
        payload = load_live_dashboard_data(
            simulation_run_id=args.simulation_run_id,
            user_id=args.user_id,
            decision_backend=args.decision_backend,
            topn=args.topn,
        )
        _print_json(payload)
        return 0

    if args.command == "performance-summary":
        payload = load_performance_summary(
            simulation_run_ids=args.simulation_run_id,
            user_id=args.user_id,
            decision_backend=args.decision_backend,
            topn=args.topn,
        )
        _print_json(payload)
        return 0

    if args.command == "simulation-results":
        payload = load_simulation_results(
            simulation_run_id=args.simulation_run_id,
            topn=args.topn,
        )
        _print_json(payload)
        return 0

    if args.command == "simulation-results-status":
        payload = load_simulation_result_status(
            simulation_run_id=args.simulation_run_id,
        )
        _print_json(payload)
        return 0

    if args.command == "profile-source-status":
        payload = load_profile_source_status(
            simulation_run_id=args.simulation_run_id,
        )
        _print_json(payload)
        return 0

    if args.command == "performance-query":
        query_payload = {
            "simulation_run_ids": args.simulation_run_id,
            "user_id": args.user_id,
            "decision_backend": args.decision_backend,
            "topn": args.topn,
            "group_by": args.group_by,
            "sort": {
                "field": args.sort_field,
                "direction": args.sort_direction,
            }
            if args.sort_field
            else None,
            "filters": {
                "decision_backend": args.decision_backend,
                "min_hands_played": args.min_hands_played,
                "max_final_rank": args.max_final_rank,
            },
        }
        if args.query_json:
            parsed_query_payload = json.loads(args.query_json)
            if isinstance(parsed_query_payload, dict):
                query_payload = parsed_query_payload
        payload = load_performance_query(
            query=query_payload,
            simulation_run_ids=args.simulation_run_id,
            user_id=args.user_id,
            decision_backend=args.decision_backend,
            min_hands_played=args.min_hands_played,
            max_final_rank=args.max_final_rank,
            topn=args.topn,
            group_by=args.group_by,
            sort={
                "field": args.sort_field,
                "direction": args.sort_direction,
            }
            if args.sort_field
            else None,
        )
        _print_json(payload)
        return 0

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
