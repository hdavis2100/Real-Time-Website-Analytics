from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from typing import Any

import pandas as pd

from poker_platform.config import get_config
from poker_platform.storage import bootstrap_backend, get_warehouse
from profiles.constants import FEATURE_COLUMNS, FEATURE_VERSION, MIN_SESSION_HANDS
from profiles.export_active_model import load_active_model_artifact
from profiles.reference_features import aggregate_session_features
from profiles.scoring import load_scoring_artifact, score_feature_vector


def score_completed_sessions(
    *,
    simulation_run_id: str | None = None,
    source_run_id: str | None = None,
    user_id: str | None = None,
    decision_backend: str | None = None,
    hero_context_hash: str | None = None,
    min_event_ts: str | None = None,
    player_id: str | None = None,
    agent_id: str | None = None,
    session_features_json: str | None = None,
    scoring_artifact_json: str | None = None,
    refresh: bool = False,
    cached_only: bool = False,
) -> dict[str, Any]:
    warehouse = _warehouse_or_default()
    artifact_wrapper = _resolve_artifact_wrapper(scoring_artifact_json, warehouse=warehouse)
    if artifact_wrapper.get("status") not in {None, "ready"}:
        return artifact_wrapper

    model_run_id = str(artifact_wrapper["model_run_id"])
    feature_version = str(artifact_wrapper["feature_version"])
    if not refresh:
        cached_results = _load_cached_results(
            warehouse,
            model_run_id=model_run_id,
            simulation_run_id=simulation_run_id,
            source_run_id=source_run_id,
            min_scored_at=min_event_ts,
            player_id=player_id,
            agent_id=agent_id,
        )
        if cached_results:
            return {
                "status": "ready",
                "cached": True,
                "model_run_id": model_run_id,
                "feature_version": feature_version,
                "simulation_run_id": simulation_run_id,
                "source_run_id": source_run_id,
                "results": cached_results,
            }
        if cached_only:
            return {
                "status": "not_found",
                "message": "No completed profile results were found",
                "simulation_run_id": simulation_run_id,
                "source_run_id": source_run_id,
            }

    if session_features_json:
        session_features = _load_provisional_session_features(
            session_features_json,
            simulation_run_id=simulation_run_id,
            source_run_id=source_run_id,
        )
        subject_rows = _filter_subject_rows(
            session_features,
            player_id=player_id,
            agent_id=agent_id,
        )
    else:
        actions, players = _load_session_frames(
            warehouse,
            simulation_run_id=simulation_run_id,
            source_run_id=source_run_id,
            user_id=user_id,
            decision_backend=decision_backend,
            hero_context_hash=hero_context_hash,
            min_event_ts=min_event_ts,
        )
        if actions.empty:
            return {
                "status": "not_found",
                "message": "No matching session actions were found",
            }

        session_features = aggregate_session_features(
            actions,
            players,
            feature_version=FEATURE_VERSION,
        )
        subject_rows = _filter_subject_rows(
            session_features,
            player_id=player_id,
            agent_id=agent_id,
        )
    if subject_rows.empty:
        return {
            "status": "not_found",
            "message": "No matching subject was found in the session",
        }

    feature_frame, result_frame = _score_subject_rows(
        warehouse,
        artifact_wrapper=artifact_wrapper,
        subject_rows=subject_rows,
    )
    _persist_session_outputs(
        warehouse,
        feature_frame=feature_frame,
        result_frame=result_frame,
    )
    return {
        "status": "ready",
        "cached": False,
        "model_run_id": model_run_id,
        "feature_version": feature_version,
        "simulation_run_id": simulation_run_id,
        "source_run_id": source_run_id,
        "results": _result_frame_to_records(result_frame),
    }


def score_completed_session(
    *,
    simulation_run_id: str | None = None,
    source_run_id: str | None = None,
    user_id: str | None = None,
    decision_backend: str | None = None,
    hero_context_hash: str | None = None,
    min_event_ts: str | None = None,
    player_id: str | None = None,
    agent_id: str | None = None,
    session_features_json: str | None = None,
    scoring_artifact_json: str | None = None,
    cached_only: bool = False,
) -> dict[str, Any]:
    payload = score_completed_sessions(
        simulation_run_id=simulation_run_id,
        source_run_id=source_run_id,
        user_id=user_id,
        decision_backend=decision_backend,
        hero_context_hash=hero_context_hash,
        min_event_ts=min_event_ts,
        player_id=player_id,
        agent_id=agent_id,
        session_features_json=session_features_json,
        scoring_artifact_json=scoring_artifact_json,
        cached_only=cached_only,
    )
    if payload.get("status") != "ready":
        return payload
    results = payload.get("results") or []
    if not results:
        return {
            "status": "not_found",
            "message": "No matching subject was found in the session",
        }
    if len(results) > 1:
        return {
            "status": "invalid_request",
            "message": "Multiple subjects exist for this session; provide player_id or agent_id",
        }
    return results[0]


def _resolve_artifact_wrapper(
    scoring_artifact_json: str | None,
    *,
    warehouse=None,
) -> dict[str, Any]:
    raw_artifact_payload = scoring_artifact_json or os.environ.get("PROFILE_SCORING_ARTIFACT_JSON")
    if raw_artifact_payload:
        artifact_payload = _extract_artifact_payload(raw_artifact_payload)
    else:
        active_model = load_active_model_artifact(warehouse=warehouse)
        if active_model.get("status") != "ready":
            return active_model
        artifact_payload = active_model["scoring_artifact"]
    wrapper = load_scoring_artifact(artifact_payload)
    wrapper["status"] = "ready"
    return wrapper


def _warehouse_or_default(warehouse=None):
    if warehouse is not None:
        return warehouse
    if os.getenv("POKER_PLATFORM_SKIP_SCHEMA_BOOTSTRAP") == "1":
        return get_warehouse(get_config())
    return bootstrap_backend()


def _normalize_optional_text(value: str | None) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _filter_optional_scope(
    frame: pd.DataFrame,
    *,
    user_id: str | None = None,
    decision_backend: str | None = None,
    hero_context_hash: str | None = None,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    for column, value in (
        ("user_id", user_id),
        ("decision_backend", decision_backend),
        ("hero_context_hash", hero_context_hash),
    ):
        normalized = _normalize_optional_text(value)
        if normalized and column in result.columns:
            result = result[result[column].astype(str) == normalized].copy()
    return result.reset_index(drop=True)


def _filter_min_timestamp(
    frame: pd.DataFrame,
    *,
    column: str,
    min_ts: str | None,
) -> pd.DataFrame:
    if frame.empty or column not in frame.columns or not min_ts:
        return frame.copy()
    threshold = pd.to_datetime(min_ts, utc=True, errors="coerce")
    if pd.isna(threshold):
        return frame.copy()
    result = frame.copy()
    parsed = pd.to_datetime(result[column], utc=True, errors="coerce")
    result = result[parsed >= threshold].copy()
    return result.reset_index(drop=True)


def _restrict_players_to_actions(players: pd.DataFrame, actions: pd.DataFrame) -> pd.DataFrame:
    if players.empty or actions.empty:
        return players.iloc[0:0].copy() if actions.empty else players.copy()
    if "hand_id" not in players.columns or "hand_id" not in actions.columns:
        return players.copy()
    hand_ids = actions["hand_id"].dropna().astype(str).drop_duplicates()
    if hand_ids.empty:
        return players.iloc[0:0].copy()
    return players[players["hand_id"].astype(str).isin(set(hand_ids))].copy().reset_index(drop=True)


def _load_session_frames(
    warehouse,
    *,
    simulation_run_id: str | None,
    source_run_id: str | None,
    user_id: str | None = None,
    decision_backend: str | None = None,
    hero_context_hash: str | None = None,
    min_event_ts: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if simulation_run_id:
        actions = warehouse.load_table("CURATED_ACTIONS", filters_eq={"simulation_run_id": simulation_run_id})
        players = warehouse.load_table("CURATED_PLAYERS", filters_eq={"simulation_run_id": simulation_run_id})
    elif source_run_id:
        actions = warehouse.load_table("CURATED_ACTIONS", filters_eq={"source_run_id": source_run_id})
        players = warehouse.load_table("CURATED_PLAYERS", filters_eq={"source_run_id": source_run_id})
    else:
        return pd.DataFrame(), pd.DataFrame()
    actions = _filter_optional_scope(
        actions,
        user_id=user_id,
        decision_backend=decision_backend,
        hero_context_hash=hero_context_hash,
    )
    actions = _filter_min_timestamp(actions, column="event_ts", min_ts=min_event_ts)
    players = _filter_optional_scope(
        players,
        user_id=user_id,
        decision_backend=decision_backend,
        hero_context_hash=hero_context_hash,
    )
    players = _restrict_players_to_actions(players, actions)
    return actions.reset_index(drop=True), players.reset_index(drop=True)


def _load_provisional_session_features(
    session_features_json: str,
    *,
    simulation_run_id: str | None,
    source_run_id: str | None,
) -> pd.DataFrame:
    parsed = json.loads(session_features_json)
    rows = parsed.get("rows") if isinstance(parsed, dict) else parsed
    if not isinstance(rows, list) or not rows:
        return pd.DataFrame()

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    if simulation_run_id and "simulation_run_id" in frame.columns:
        frame = frame[
            frame["simulation_run_id"].astype(str) == str(simulation_run_id)
        ].copy()
    if source_run_id and "source_run_id" in frame.columns:
        frame = frame[
            frame["source_run_id"].astype(str) == str(source_run_id)
        ].copy()
    return frame.reset_index(drop=True)


def _filter_subject_rows(
    session_features: pd.DataFrame,
    *,
    player_id: str | None,
    agent_id: str | None,
) -> pd.DataFrame:
    if session_features.empty:
        return pd.DataFrame()
    frame = session_features.copy()
    if player_id:
        frame = frame[frame["player_id"] == player_id]
    if agent_id:
        frame = frame[frame["agent_id"] == agent_id]
    if frame.empty:
        return pd.DataFrame()
    return frame.reset_index(drop=True)


def _extract_artifact_payload(payload: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload, dict):
        parsed = payload
    else:
        parsed = json.loads(payload)
    if "scoring_artifact" in parsed:
        return parsed["scoring_artifact"]
    return parsed


def _load_cached_results(
    warehouse,
    *,
    model_run_id: str,
    simulation_run_id: str | None,
    source_run_id: str | None,
    min_scored_at: str | None,
    player_id: str | None,
    agent_id: str | None,
) -> list[dict[str, Any]]:
    filters_eq: dict[str, Any] = {"model_run_id": model_run_id}
    if simulation_run_id:
        filters_eq["simulation_run_id"] = simulation_run_id
    if source_run_id:
        filters_eq["source_run_id"] = source_run_id
    if player_id:
        filters_eq["player_id"] = player_id
    if agent_id:
        filters_eq["agent_id"] = agent_id
    frame = warehouse.load_table(
        "PROFILE_SESSION_RESULTS",
        filters_eq=filters_eq,
        order_by=[("player_id", True), ("agent_id", True)],
    )
    if frame.empty:
        return []
    frame = _filter_min_timestamp(frame, column="scored_at", min_ts=min_scored_at)
    if frame.empty:
        return []
    return _result_frame_to_records(frame)


def _load_prototypes(warehouse, *, model_run_id: str, cluster_id: int) -> list[dict[str, Any]]:
    frame = warehouse.load_table(
        "PROFILE_CLUSTER_PROTOTYPES",
        filters_eq={"model_run_id": model_run_id, "cluster_id": cluster_id},
        order_by=[("prototype_rank", True)],
        limit=3,
    )
    if frame.empty:
        return []
    rows = []
    for row in frame.to_dict(orient="records"):
        prototype = row.get("prototype_json")
        if isinstance(prototype, str):
            prototype = json.loads(prototype)
        rows.append(
            {
                "prototype_rank": int(row["prototype_rank"]),
                "player_id": row["player_id"],
                "source_dataset": row["source_dataset"],
                "hands_played": int(row["hands_played"]),
                "distance_to_centroid": float(row["distance_to_centroid"]),
                "prototype": prototype or {},
            }
        )
    return rows


def _score_subject_rows(
    warehouse,
    *,
    artifact_wrapper: dict[str, Any],
    subject_rows: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    model_run_id = str(artifact_wrapper["model_run_id"])
    feature_version = str(artifact_wrapper["feature_version"])
    feature_frame = subject_rows.copy()
    feature_frame["model_run_id"] = model_run_id
    feature_frame["feature_version"] = feature_version
    feature_frame["scored_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

    result_rows: list[dict[str, Any]] = []
    for subject_row in feature_frame.to_dict(orient="records"):
        hands_observed = int(subject_row["hands_observed"])
        feature_vector = {column: float(subject_row[column]) for column in FEATURE_COLUMNS}
        if hands_observed < MIN_SESSION_HANDS:
            result_rows.append(
                {
                    "model_run_id": model_run_id,
                    "feature_version": feature_version,
                    "source_type": subject_row["source_type"],
                    "source_dataset": subject_row["source_dataset"],
                    "source_run_id": subject_row["source_run_id"],
                    "simulation_run_id": subject_row["simulation_run_id"],
                    "subject_id": subject_row["subject_id"],
                    "player_id": subject_row["player_id"],
                    "agent_id": subject_row["agent_id"],
                    "hands_observed": hands_observed,
                    "status": "insufficient_evidence",
                    "cluster_id": None,
                    "cluster_label": None,
                    "confidence_score": 0.0,
                    "summary_text": f"Need at least {MIN_SESSION_HANDS} hands to produce a stable profile",
                    "feature_vector": feature_vector,
                    "top_feature_deltas": [],
                    "prototype_matches": [],
                    "scored_at": datetime.now(timezone.utc).replace(tzinfo=None),
                }
            )
            continue

        scored = score_feature_vector(
            {**feature_vector, "hands_observed": hands_observed},
            artifact_wrapper,
        )
        prototypes = _load_prototypes(
            warehouse,
            model_run_id=model_run_id,
            cluster_id=int(scored["cluster_id"]),
        )
        result_rows.append(
            {
                "model_run_id": model_run_id,
                "feature_version": feature_version,
                "source_type": subject_row["source_type"],
                "source_dataset": subject_row["source_dataset"],
                "source_run_id": subject_row["source_run_id"],
                "simulation_run_id": subject_row["simulation_run_id"],
                "subject_id": subject_row["subject_id"],
                "player_id": subject_row["player_id"],
                "agent_id": subject_row["agent_id"],
                "hands_observed": hands_observed,
                "status": "ready",
                "cluster_id": int(scored["cluster_id"]),
                "cluster_label": scored["cluster_label"],
                "confidence_score": float(scored["confidence_score"]),
                "summary_text": scored["summary_text"],
                "feature_vector": scored["feature_vector"],
                "top_feature_deltas": scored["top_feature_deltas"],
                "prototype_matches": prototypes,
                "scored_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
        )
    return feature_frame, pd.DataFrame(result_rows)


def _persist_session_outputs(warehouse, *, feature_frame: pd.DataFrame, result_frame: pd.DataFrame) -> None:
    if feature_frame.empty or result_frame.empty:
        return
    key_frame = feature_frame[
        ["model_run_id", "source_run_id", "simulation_run_id", "subject_id"]
    ].drop_duplicates()
    warehouse.delete_matching_keys(
        "PROFILE_SESSION_FEATURES",
        key_frame,
        ["model_run_id", "source_run_id", "simulation_run_id", "subject_id"],
    )
    warehouse.write_dataframe("PROFILE_SESSION_FEATURES", feature_frame, force_bulk=True)
    warehouse.delete_matching_keys(
        "PROFILE_SESSION_RESULTS",
        result_frame,
        ["model_run_id", "source_run_id", "simulation_run_id", "subject_id"],
    )
    warehouse.write_dataframe("PROFILE_SESSION_RESULTS", result_frame, force_bulk=True)


def _json_field(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return value
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return value
    return value


def _result_frame_to_records(result_frame: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in result_frame.to_dict(orient="records"):
        record = dict(row)
        for field in ["feature_vector", "top_feature_deltas", "prototype_matches"]:
            record[field] = _json_field(record.get(field))
        record["model_run_id"] = str(record["model_run_id"])
        record["status"] = str(record["status"])
        if record.get("cluster_id") is not None:
            record["cluster_id"] = int(record["cluster_id"])
        if record.get("confidence_score") is not None:
            record["confidence_score"] = float(record["confidence_score"])
        if record.get("hands_observed") is not None:
            record["hands_observed"] = int(record["hands_observed"])
        records.append(record)
    return records


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Score one completed session against the active archetype model.")
    parser.add_argument("--simulation-run-id", default=None)
    parser.add_argument("--source-run-id", default=None)
    parser.add_argument("--user-id", default=None)
    parser.add_argument("--decision-backend", default=None)
    parser.add_argument("--hero-context-hash", default=None)
    parser.add_argument("--min-event-ts", default=None)
    parser.add_argument("--player-id", default=None)
    parser.add_argument("--agent-id", default=None)
    parser.add_argument("--session-features-json", default=None)
    parser.add_argument("--scoring-artifact-json", default=None)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--cached-only", action="store_true")
    parser.add_argument("--all-subjects", action="store_true")
    args = parser.parse_args(argv)

    if args.all_subjects:
        result = score_completed_sessions(
            simulation_run_id=args.simulation_run_id,
            source_run_id=args.source_run_id,
            user_id=args.user_id,
            decision_backend=args.decision_backend,
            hero_context_hash=args.hero_context_hash,
            min_event_ts=args.min_event_ts,
            player_id=args.player_id,
            agent_id=args.agent_id,
            session_features_json=args.session_features_json,
            scoring_artifact_json=args.scoring_artifact_json,
            refresh=args.refresh,
            cached_only=args.cached_only,
        )
    else:
        result = score_completed_session(
            simulation_run_id=args.simulation_run_id,
            source_run_id=args.source_run_id,
            user_id=args.user_id,
            decision_backend=args.decision_backend,
            hero_context_hash=args.hero_context_hash,
            min_event_ts=args.min_event_ts,
            player_id=args.player_id,
            agent_id=args.agent_id,
            session_features_json=args.session_features_json,
            scoring_artifact_json=args.scoring_artifact_json,
            cached_only=args.cached_only,
        )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
