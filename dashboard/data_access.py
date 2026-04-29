from __future__ import annotations

from dataclasses import dataclass
import os
import re
from typing import Sequence

import pandas as pd

from poker_platform.config import get_config
from poker_platform.redis_live import RedisLiveStore, get_live_store
from poker_platform.storage import get_warehouse

TERMINAL_RUN_STATUSES = {"completed", "failed"}


@dataclass(slots=True)
class TableLoadResult:
    table_name: str
    dataframe: pd.DataFrame
    source: str
    location: str = ""
    message: str = ""

    @property
    def is_missing(self) -> bool:
        return self.dataframe.empty and self.source == "missing"


@dataclass(slots=True)
class LiveDashboardResult:
    available: bool
    source: str
    message: str = ""
    active_run_ids: list[str] | None = None
    active_runs: list[dict[str, object]] | None = None
    selected_run_id: str | None = None
    selected_run_meta: dict[str, object] | None = None
    run_profit_leaderboard: list[dict[str, object]] | None = None
    run_bb_per_100_leaderboard: list[dict[str, object]] | None = None
    run_high_hand_leaderboard: list[dict[str, object]] | None = None
    global_profit_leaderboard: list[dict[str, object]] | None = None
    global_high_hand_leaderboard: list[dict[str, object]] | None = None
    global_hero_context_leaderboard: list[dict[str, object]] | None = None
    run_agents: list[dict[str, object]] | None = None
    hero_profit_timeseries: list[dict[str, object]] | None = None


def warehouse_location() -> str:
    return f"{os.environ.get('SNOWFLAKE_DATABASE', 'snowflake')}.{os.environ.get('SNOWFLAKE_SCHEMA', 'public')}"


def load_table(table_name: str, *, limit: int | None = None) -> TableLoadResult:
    warehouse = get_warehouse(get_config())
    location = warehouse_location()
    try:
        frame = warehouse.load_table(table_name, limit=limit)
    except Exception as exc:
        return TableLoadResult(table_name, pd.DataFrame(), "missing", location, str(exc))
    return TableLoadResult(table_name, frame, "snowflake", location, "loaded from Snowflake")


def table_or_empty(table_name: str, *, limit: int | None = None) -> pd.DataFrame:
    return load_table(table_name, limit=limit).dataframe


def safe_numeric(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    for column in columns:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    return result


def _matches_filter(value: object, expected: str | None) -> bool:
    if expected in (None, "", "All"):
        return True
    if value is None:
        return False
    return str(value) == str(expected)


def _is_live_status(status: object) -> bool:
    return str(status or "").strip().lower() not in TERMINAL_RUN_STATUSES


def _filter_rows(
    rows: Sequence[dict[str, object]],
    *,
    user_id: str | None = None,
    decision_backend: str | None = None,
) -> list[dict[str, object]]:
    return [
        dict(row)
        for row in rows
        if _matches_filter(row.get("user_id"), user_id)
        and _matches_filter(row.get("decision_backend"), decision_backend)
    ]


def _load_run_metas(store: RedisLiveStore, run_ids: Sequence[str]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for run_id in run_ids:
        meta = store.get_run_meta(run_id)
        if not meta:
            continue
        normalized = dict(meta)
        normalized["simulation_run_id"] = str(
            normalized.get("simulation_run_id") or normalized.get("run_id") or run_id
        )
        rows.append(normalized)
    return rows


def _load_run_meta(store: RedisLiveStore, run_id: str | None) -> dict[str, object] | None:
    if not run_id:
        return None
    rows = _load_run_metas(store, [run_id])
    return rows[0] if rows else None


def _extract_hand_order(value: object) -> tuple[int, str]:
    text = str(value or "").strip()
    if not text:
        return (10**9, "")
    match = re.search(r"(\d+)(?!.*\d)", text)
    if match:
        return (int(match.group(1)), text)
    return (10**9, text)


def _hero_rows(rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    normalized = [dict(row) for row in rows if isinstance(row, dict)]
    explicit = [
        row
        for row in normalized
        if bool(row.get("is_hero_player"))
        or str(row.get("player_id") or "").strip() == "user_agent"
        or str(row.get("agent_id") or "").strip() == "user_agent"
    ]
    if explicit:
        return explicit
    unlabeled = [
        row
        for row in normalized
        if not str(row.get("persona_name") or "").strip()
    ]
    if len(unlabeled) == 1:
        return unlabeled
    return []


def _participant_key(row: dict[str, object]) -> tuple[str, str]:
    return (
        str(row.get("player_id") or "").strip(),
        str(row.get("agent_id") or "").strip(),
    )


def _merge_agent_rows_with_cumulative_totals(
    run_agents: Sequence[dict[str, object]],
    cumulative_rows: Sequence[dict[str, object]],
) -> list[dict[str, object]]:
    cumulative_by_key = {
        key: dict(row)
        for row in cumulative_rows
        if (key := _participant_key(dict(row))) != ("", "")
    }
    merged_rows: list[dict[str, object]] = []
    for row in run_agents:
        merged = dict(row)
        row_hands_played = _max_numeric([merged], "hands_played")
        if row_hands_played is not None:
            observed_hands = _max_numeric([merged], "observed_hands") or 0
            merged["observed_hands"] = int(max(observed_hands, row_hands_played))
        row_total_bb_won = _max_numeric([merged], "total_bb_won")
        if row_total_bb_won is not None and merged.get("bb_won") in (None, ""):
            merged["bb_won"] = float(row_total_bb_won)
        cumulative = cumulative_by_key.get(_participant_key(merged))
        if cumulative:
            hands_played = _max_numeric([cumulative], "hands_played")
            if hands_played is not None:
                merged["hands_played"] = int(hands_played)
                observed_hands = _max_numeric([merged], "observed_hands") or 0
                merged["observed_hands"] = int(max(observed_hands, hands_played))
            total_bb_won = _max_numeric([cumulative], "total_bb_won")
            if total_bb_won is not None:
                merged["total_bb_won"] = float(total_bb_won)
                merged["bb_won"] = float(total_bb_won)
            for key in [
                "bb_per_100",
                "final_rank",
                "seat",
                "backend_type",
                "persona_name",
                "model_name",
                "hero_context_preview",
            ]:
                if merged.get(key) in (None, "") and cumulative.get(key) not in (None, ""):
                    merged[key] = cumulative[key]
        merged_rows.append(merged)
    return merged_rows


def _hero_profit_timeseries(rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    hero_rows = _hero_rows(rows)
    if not hero_rows:
        return []

    ordered = sorted(
        hero_rows,
        key=lambda row: _extract_hand_order(row.get("hand_id")),
    )
    cumulative = 0.0
    points: list[dict[str, object]] = []
    for index, row in enumerate(ordered, start=1):
        try:
            result_bb = float(row.get("result_bb"))
        except (TypeError, ValueError):
            result_bb = 0.0
        cumulative += result_bb
        points.append(
            {
                "simulation_run_id": row.get("simulation_run_id"),
                "hand_id": row.get("hand_id"),
                "hand_number": index,
                "result_bb": result_bb,
                "cumulative_bb_won": round(cumulative, 4),
            }
        )
    return points


def _timeseries_max_hand_number(rows: Sequence[dict[str, object]]) -> int:
    winner = 0
    for index, row in enumerate(rows, start=1):
        try:
            hand_number = int(float(row.get("hand_number")))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            hand_number = index
        winner = max(winner, hand_number)
    return winner


def _coarse_hero_profit_timeseries(
    selected_run_meta: dict[str, object] | None,
    rows: Sequence[dict[str, object]],
) -> list[dict[str, object]]:
    hero_rows = _hero_rows(rows)
    if not hero_rows:
        return []

    hero_row = max(
        hero_rows,
        key=lambda row: _max_numeric([row], "observed_hands")
        or _max_numeric([row], "hands_played")
        or 0,
    )
    expected_hands = int(
        max(
            _max_numeric([selected_run_meta or {}], "published_hand_summaries") or 0,
            _max_numeric([hero_row], "observed_hands") or 0,
            _max_numeric([hero_row], "hands_played") or 0,
        )
    )
    if expected_hands <= 0:
        return []

    total_bb_won = _max_numeric([hero_row], "bb_won")
    if total_bb_won is None:
        total_bb_won = _max_numeric([hero_row], "total_bb_won")
    if total_bb_won is None:
        return []

    simulation_run_id = (
        hero_row.get("simulation_run_id")
        or (selected_run_meta or {}).get("simulation_run_id")
    )
    return [
        {
            "simulation_run_id": simulation_run_id,
            "hand_number": 0,
            "cumulative_bb_won": 0.0,
        },
        {
            "simulation_run_id": simulation_run_id,
            "hand_number": expected_hands,
            "cumulative_bb_won": round(float(total_bb_won), 4),
        },
    ]


def _hero_profit_timeseries_is_stale(
    rows: Sequence[dict[str, object]],
    *,
    selected_run_meta: dict[str, object] | None,
    run_agents: Sequence[dict[str, object]],
) -> bool:
    if not rows:
        return True
    hero_rows = _hero_rows(run_agents)
    expected_hands = max(
        _max_numeric([selected_run_meta or {}], "published_hand_summaries") or 0,
        _max_numeric(hero_rows, "observed_hands") or 0,
        _max_numeric(hero_rows, "hands_played") or 0,
    )
    if expected_hands < 10:
        return False
    return _timeseries_max_hand_number(rows) < max(5, int(expected_hands * 0.75))


def _first_present(rows: Sequence[dict[str, object]], key: str) -> object | None:
    for row in rows:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _max_numeric(rows: Sequence[dict[str, object]], key: str) -> float | None:
    winner: float | None = None
    for row in rows:
        try:
            candidate = float(row.get(key))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        winner = candidate if winner is None else max(winner, candidate)
    return winner


def _status_rank(status: object) -> int:
    return 0 if str(status or "").strip().lower() == "queued" else 1


def _preferred_live_status(rows: Sequence[dict[str, object]]) -> str:
    winner = "running"
    winner_rank = _status_rank(winner)
    for row in rows:
        candidate = str(row.get("status") or "").strip().lower()
        if not candidate:
            continue
        candidate_rank = _status_rank(candidate)
        if candidate_rank >= winner_rank:
            winner = candidate
            winner_rank = candidate_rank
    return winner


def _synthesize_selected_run_meta(
    *,
    simulation_run_id: str,
    selected_run_meta: dict[str, object] | None,
    active_runs: Sequence[dict[str, object]],
    run_profit: Sequence[dict[str, object]],
    run_bb_per_100: Sequence[dict[str, object]],
    run_high_hand: Sequence[dict[str, object]],
    run_agents: Sequence[dict[str, object]],
    hero_profit_timeseries: Sequence[dict[str, object]],
) -> dict[str, object] | None:
    if selected_run_meta is not None:
        return selected_run_meta

    active_match = next(
        (
            dict(run)
            for run in active_runs
            if str(run.get("simulation_run_id") or "").strip() == str(simulation_run_id).strip()
        ),
        None,
    )
    candidate_rows = [
        *[dict(row) for row in run_profit],
        *[dict(row) for row in run_bb_per_100],
        *[dict(row) for row in run_high_hand],
        *[dict(row) for row in run_agents],
    ]
    if active_match is None and not candidate_rows and not list(hero_profit_timeseries):
        return None

    synthesized = active_match or {}
    synthesized["simulation_run_id"] = str(simulation_run_id)
    if not str(synthesized.get("status") or "").strip():
        synthesized["status"] = _preferred_live_status(candidate_rows)

    for key in [
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "hero_context_preview",
        "backend_type",
        "model_name",
        "updated_at",
        "started_at",
    ]:
        if synthesized.get(key) not in (None, ""):
            continue
        value = _first_present(candidate_rows, key)
        if value not in (None, ""):
            synthesized[key] = value

    if synthesized.get("published_hand_summaries") in (None, ""):
        hand_number_max = _max_numeric(hero_profit_timeseries, "hand_number")
        hands_played_max = _max_numeric(run_agents, "hands_played")
        published_hand_summaries = hand_number_max or hands_played_max
        if published_hand_summaries is not None:
            synthesized["published_hand_summaries"] = int(published_hand_summaries)

    if synthesized.get("published_actions") in (None, ""):
        observed_actions_max = _max_numeric(run_agents, "observed_actions")
        if observed_actions_max is not None:
            synthesized["published_actions"] = int(observed_actions_max)

    return synthesized


def load_live_dashboard_data(
    *,
    simulation_run_id: str | None = None,
    user_id: str | None = None,
    decision_backend: str | None = None,
    topn: int = 25,
    live_store: RedisLiveStore | None = None,
) -> LiveDashboardResult:
    store = live_store or get_live_store(get_config())
    try:
        active_run_ids = store.list_active_runs(user_id=user_id)
        active_runs = _load_run_metas(store, active_run_ids)
        active_runs = [
            run
            for run in active_runs
            if _is_live_status(run.get("status"))
            and _matches_filter(run.get("decision_backend"), decision_backend)
        ]
    except Exception:
        return LiveDashboardResult(
            available=False,
            source="redis",
            message="Live data is temporarily unavailable.",
            active_run_ids=[],
            active_runs=[],
        )

    selected_run_id = simulation_run_id
    if selected_run_id is None and active_runs:
        selected_run_id = str(active_runs[0]["simulation_run_id"])
    selected_run_meta = None
    if selected_run_id is not None:
        candidate = _load_run_meta(store, str(selected_run_id))
        if candidate and _matches_filter(candidate.get("user_id"), user_id) and _matches_filter(
            candidate.get("decision_backend"), decision_backend
        ):
            selected_run_meta = candidate

    run_profit: list[dict[str, object]] = []
    run_bb_per_100: list[dict[str, object]] = []
    run_high_hand: list[dict[str, object]] = []
    run_agents: list[dict[str, object]] = []
    hero_profit_timeseries: list[dict[str, object]] = []
    if selected_run_id is not None:
        run_profit = _filter_rows(
            store.get_run_leaderboard(selected_run_id, "profit", topn=topn),
            user_id=user_id,
            decision_backend=decision_backend,
        )
        run_bb_per_100 = _filter_rows(
            store.get_run_leaderboard(selected_run_id, "bb_per_100", topn=topn),
            user_id=user_id,
            decision_backend=decision_backend,
        )
        run_high_hand = _filter_rows(
            store.get_run_leaderboard(selected_run_id, "high_hand", topn=topn),
            user_id=user_id,
            decision_backend=decision_backend,
        )
        run_agents = _filter_rows(
            store.get_run_agents(selected_run_id),
            user_id=user_id,
            decision_backend=decision_backend,
        )
        cumulative_agent_rows = _filter_rows(
            store.list_json_hash(f"run:{selected_run_id}:state:player_aggregates"),
            user_id=user_id,
            decision_backend=decision_backend,
        )
        run_agents = _merge_agent_rows_with_cumulative_totals(
            run_agents,
            cumulative_agent_rows or run_profit,
        )
        hero_profit_timeseries = _hero_profit_timeseries(
            _filter_rows(
                store.list_json_hash(f"run:{selected_run_id}:state:player_hands"),
                user_id=user_id,
                decision_backend=decision_backend,
            )
        )
        selected_run_meta = _synthesize_selected_run_meta(
            simulation_run_id=str(selected_run_id),
            selected_run_meta=selected_run_meta,
            active_runs=active_runs,
            run_profit=run_profit,
            run_bb_per_100=run_bb_per_100,
            run_high_hand=run_high_hand,
            run_agents=run_agents,
            hero_profit_timeseries=hero_profit_timeseries,
        )
        if _hero_profit_timeseries_is_stale(
            hero_profit_timeseries,
            selected_run_meta=selected_run_meta,
            run_agents=run_agents,
        ):
            coarse_profit_timeseries = _coarse_hero_profit_timeseries(
                selected_run_meta,
                run_agents,
            )
            if coarse_profit_timeseries:
                hero_profit_timeseries = coarse_profit_timeseries

    if not active_runs and selected_run_meta is None:
        return LiveDashboardResult(
            available=False,
            source="redis",
            message="No active simulation runs are available yet.",
            active_run_ids=[],
            active_runs=[],
        )

    if selected_run_meta is None and selected_run_id is not None:
        selected_run_meta = next(
            (run for run in active_runs if str(run.get("simulation_run_id")) == str(selected_run_id)),
            None,
        )
    if selected_run_meta is None and selected_run_id is not None:
        candidate = _load_run_meta(store, str(selected_run_id))
        if candidate and _matches_filter(candidate.get("user_id"), user_id) and _matches_filter(
            candidate.get("decision_backend"), decision_backend
        ):
            selected_run_meta = candidate
    if selected_run_meta is None:
        return LiveDashboardResult(
            available=False,
            source="redis",
            message="The selected run is not active right now.",
            active_run_ids=[str(run["simulation_run_id"]) for run in active_runs],
            active_runs=active_runs,
            selected_run_id=selected_run_id,
        )

    global_profit = _filter_rows(
        store.get_global_leaderboard("profit", topn=topn),
        user_id=user_id,
        decision_backend=decision_backend,
    )
    global_high_hand = _filter_rows(
        store.get_global_leaderboard("high_hand", topn=topn),
        user_id=user_id,
        decision_backend=decision_backend,
    )
    global_hero_context = _filter_rows(
        store.get_global_leaderboard("hero_context", topn=topn),
        user_id=user_id,
        decision_backend=decision_backend,
    )
    return LiveDashboardResult(
        available=True,
        source="redis",
        active_run_ids=[str(run["simulation_run_id"]) for run in active_runs],
        active_runs=active_runs,
        selected_run_id=selected_run_id,
        selected_run_meta=selected_run_meta,
        run_profit_leaderboard=run_profit,
        run_bb_per_100_leaderboard=run_bb_per_100,
        run_high_hand_leaderboard=run_high_hand,
        global_profit_leaderboard=global_profit,
        global_high_hand_leaderboard=global_high_hand,
        global_hero_context_leaderboard=global_hero_context,
        run_agents=run_agents,
        hero_profit_timeseries=hero_profit_timeseries,
    )
