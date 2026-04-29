from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

import pandas as pd

from profiles.constants import (
    DEFAULT_TRAINING_DATASET,
    FEATURE_COLUMNS,
    FEATURE_VERSION,
    MIN_SESSION_HANDS,
    MIN_TRAINING_HANDS,
    PROFILE_PLAYER_HAND_FEATURE_COLUMNS,
    PROFILE_PLAYER_TRAINING_FEATURE_COLUMNS,
    PROFILE_SESSION_FEATURE_COLUMNS,
    SIMULATED_POSTFLOP_BET_POT_RATIO_CAP,
    SIMULATED_PREFLOP_RAISE_BB_CAP,
)


AGGRESSIVE_ACTIONS = {"bet", "raise", "all_in"}
PASSIVE_ACTIONS = {"call"}
VPIP_ACTIONS = {"call", "bet", "raise", "all_in"}
LATE_POSITIONS = {"CO", "BTN"}
BLIND_POSITIONS = {"SB", "BB"}
POSTFLOP_STREETS = {"flop", "turn", "river"}

ACTION_JOIN_COLUMNS = [
    "hand_id",
    "player_id",
    "source_run_id",
    "source_type",
    "source_dataset",
    "simulation_run_id",
]

HAND_KEY_COLUMNS = [
    "hand_id",
    "source_run_id",
    "source_type",
    "source_dataset",
    "simulation_run_id",
    "table_id",
]


def compute_player_hand_features(
    actions: pd.DataFrame,
    players: pd.DataFrame | None = None,
    *,
    training_dataset: str = DEFAULT_TRAINING_DATASET,
    feature_version: str = FEATURE_VERSION,
) -> pd.DataFrame:
    prepared_actions = _prepare_actions(actions)
    if prepared_actions.empty:
        return pd.DataFrame(columns=PROFILE_PLAYER_HAND_FEATURE_COLUMNS)

    prepared_players = _prepare_players(players)
    prepared_actions = _attach_player_stacks(prepared_actions, prepared_players)

    preflop_aggressor = _street_aggressor_map(prepared_actions, street="preflop", last_aggressor=True)
    flop_first_aggressor = _street_aggressor_map(prepared_actions, street="flop", last_aggressor=False)
    turn_first_aggressor = _street_aggressor_map(prepared_actions, street="turn", last_aggressor=False)

    rows: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    group_columns = [*HAND_KEY_COLUMNS, "player_id"]
    grouped = prepared_actions.sort_values(["hand_id", "action_index"]).groupby(group_columns, dropna=False, sort=False)
    for keys, frame in grouped:
        key_map = dict(zip(group_columns, keys if isinstance(keys, tuple) else (keys,), strict=True))
        hand_key = _key_tuple(key_map[column] for column in HAND_KEY_COLUMNS)
        position = _first_non_null(frame["position"], default="UNKNOWN")
        stack_start_bb = float(frame["stack_start_bb"].dropna().max()) if frame["stack_start_bb"].notna().any() else 0.0
        preflop_mask = frame["street"] == "preflop"
        aggressive_mask = frame["action_type"].isin(AGGRESSIVE_ACTIONS)
        passive_mask = frame["action_type"].isin(PASSIVE_ACTIONS)
        preflop_aggressive = preflop_mask & aggressive_mask
        postflop_aggressive = frame["street"].isin(POSTFLOP_STREETS) & aggressive_mask & (frame["pot_before_bb"] > 0)
        saw_flop = bool(frame["street"].isin({"flop", "turn", "river", "showdown"}).any())
        saw_turn = bool(frame["street"].isin({"turn", "river", "showdown"}).any())
        saw_river = bool(frame["street"].isin({"river", "showdown"}).any())
        player_id = str(key_map["player_id"])
        preflop_aggressive_amounts = frame.loc[preflop_aggressive, "amount_bb"].fillna(0.0)
        postflop_aggressive_bet_ratios = (
            frame.loc[postflop_aggressive, "amount_bb"].fillna(0.0) / frame.loc[postflop_aggressive, "pot_before_bb"]
        )
        if str(key_map["source_type"] or "").strip().lower() == "simulated":
            preflop_aggressive_amounts = preflop_aggressive_amounts.clip(upper=SIMULATED_PREFLOP_RAISE_BB_CAP)
            postflop_aggressive_bet_ratios = postflop_aggressive_bet_ratios.clip(
                upper=SIMULATED_POSTFLOP_BET_POT_RATIO_CAP
            )

        row = {
            "training_dataset": training_dataset,
            "feature_version": feature_version,
            "source_type": key_map["source_type"],
            "source_dataset": key_map["source_dataset"],
            "source_run_id": key_map["source_run_id"],
            "simulation_run_id": key_map["simulation_run_id"],
            "table_id": key_map["table_id"],
            "hand_id": key_map["hand_id"],
            "player_id": player_id,
            "agent_id": _first_non_null(frame["agent_id"]),
            "position": position,
            "stack_start_bb": stack_start_bb,
            "preflop_vpip": bool((preflop_mask & frame["action_type"].isin(VPIP_ACTIONS)).any()),
            "preflop_pfr": bool(preflop_aggressive.any()),
            "aggressive_actions": int(aggressive_mask.sum()),
            "passive_actions": int(passive_mask.sum()),
            "preflop_aggressive_amount_total_bb": float(preflop_aggressive_amounts.sum()),
            "preflop_aggressive_action_count": int(preflop_aggressive.sum()),
            "postflop_aggressive_bet_pot_ratio_sum": float(postflop_aggressive_bet_ratios.sum()),
            "postflop_aggressive_bet_count": int(postflop_aggressive.sum()),
            "saw_flop": saw_flop,
            "saw_turn": saw_turn,
            "saw_river": saw_river,
            "all_in": bool(frame["is_all_in"].fillna(False).any()),
            "cbet_opportunity_flop": bool(saw_flop and preflop_aggressor.get(hand_key) == player_id),
            "flop_cbet": bool(saw_flop and preflop_aggressor.get(hand_key) == player_id and flop_first_aggressor.get(hand_key) == player_id),
            "turn_barrel_opportunity": bool(
                saw_turn and preflop_aggressor.get(hand_key) == player_id and flop_first_aggressor.get(hand_key) == player_id
            ),
            "turn_barrel": bool(
                saw_turn
                and preflop_aggressor.get(hand_key) == player_id
                and flop_first_aggressor.get(hand_key) == player_id
                and turn_first_aggressor.get(hand_key) == player_id
            ),
            "late_position_hand": position in LATE_POSITIONS,
            "late_position_vpip": bool(position in LATE_POSITIONS and (preflop_mask & frame["action_type"].isin(VPIP_ACTIONS)).any()),
            "blind_position_hand": position in BLIND_POSITIONS,
            "blind_defend": bool(position in BLIND_POSITIONS and (preflop_mask & frame["action_type"].isin(VPIP_ACTIONS)).any()),
            "short_stack_hand": stack_start_bb <= 20.0,
            "short_stack_aggressive_actions": int(aggressive_mask.sum()) if stack_start_bb <= 20.0 else 0,
            "short_stack_passive_actions": int(passive_mask.sum()) if stack_start_bb <= 20.0 else 0,
            "deep_stack_hand": stack_start_bb >= 80.0,
            "deep_stack_vpip": bool(stack_start_bb >= 80.0 and (preflop_mask & frame["action_type"].isin(VPIP_ACTIONS)).any()),
            "updated_at": now,
        }
        rows.append(row)

    return pd.DataFrame(rows, columns=PROFILE_PLAYER_HAND_FEATURE_COLUMNS)


def aggregate_training_features(
    hand_features: pd.DataFrame,
    *,
    training_dataset: str = DEFAULT_TRAINING_DATASET,
    feature_version: str = FEATURE_VERSION,
    min_hands: int = MIN_TRAINING_HANDS,
) -> pd.DataFrame:
    if hand_features.empty:
        return pd.DataFrame(columns=PROFILE_PLAYER_TRAINING_FEATURE_COLUMNS)

    rows: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    grouped = hand_features.groupby(["player_id", "source_dataset"], dropna=False, sort=False)
    for keys, frame in grouped:
        player_id, source_dataset = keys if isinstance(keys, tuple) else (keys, None)
        hands_played = int(frame["hand_id"].nunique())
        aggressive_actions = int(frame["aggressive_actions"].sum())
        passive_actions = int(frame["passive_actions"].sum())
        flop_cbet_opp = int(frame["cbet_opportunity_flop"].sum())
        turn_barrel_opp = int(frame["turn_barrel_opportunity"].sum())
        late_position_hands = int(frame["late_position_hand"].sum())
        blind_position_hands = int(frame["blind_position_hand"].sum())
        short_stack_hands = int(frame["short_stack_hand"].sum())
        deep_stack_hands = int(frame["deep_stack_hand"].sum())
        preflop_raise_events = int(frame["preflop_aggressive_action_count"].sum())
        postflop_bet_events = int(frame["postflop_aggressive_bet_count"].sum())

        rows.append(
            {
                "training_dataset": training_dataset,
                "feature_version": feature_version,
                "player_id": player_id,
                "source_dataset": source_dataset,
                "hands_played": hands_played,
                "aggressive_actions": aggressive_actions,
                "passive_actions": passive_actions,
                "flop_cbet_opportunities": flop_cbet_opp,
                "turn_barrel_opportunities": turn_barrel_opp,
                "late_position_hands": late_position_hands,
                "blind_position_hands": blind_position_hands,
                "short_stack_hands": short_stack_hands,
                "deep_stack_hands": deep_stack_hands,
                "vpip_rate": _smoothed_ratio(frame["preflop_vpip"].sum(), hands_played),
                "pfr_rate": _smoothed_ratio(frame["preflop_pfr"].sum(), hands_played),
                "aggression_frequency": _smoothed_ratio(aggressive_actions, aggressive_actions + passive_actions, smooth=False),
                "call_preference": _smoothed_ratio(passive_actions, aggressive_actions + passive_actions, smooth=False),
                "flop_cbet_rate": _smoothed_ratio(frame["flop_cbet"].sum(), flop_cbet_opp),
                "turn_barrel_rate": _smoothed_ratio(frame["turn_barrel"].sum(), turn_barrel_opp),
                "river_seen_rate": _smoothed_ratio(frame["saw_river"].sum(), hands_played),
                "all_in_rate": _smoothed_ratio(frame["all_in"].sum(), hands_played),
                "avg_preflop_raise_bb": float(frame["preflop_aggressive_amount_total_bb"].sum()) / preflop_raise_events
                if preflop_raise_events
                else 0.0,
                "avg_postflop_bet_pot_ratio": float(frame["postflop_aggressive_bet_pot_ratio_sum"].sum()) / postflop_bet_events
                if postflop_bet_events
                else 0.0,
                "late_position_vpip_rate": _smoothed_ratio(frame["late_position_vpip"].sum(), late_position_hands),
                "blind_defend_rate": _smoothed_ratio(frame["blind_defend"].sum(), blind_position_hands),
                "short_stack_aggression_rate": _smoothed_ratio(
                    frame["short_stack_aggressive_actions"].sum(),
                    frame["short_stack_aggressive_actions"].sum() + frame["short_stack_passive_actions"].sum(),
                    smooth=False,
                ),
                "deep_stack_looseness": _smoothed_ratio(frame["deep_stack_vpip"].sum(), deep_stack_hands),
                "eligible_for_training": hands_played >= min_hands,
                "updated_at": now,
            }
        )

    return pd.DataFrame(rows, columns=PROFILE_PLAYER_TRAINING_FEATURE_COLUMNS)


def aggregate_session_features(
    actions: pd.DataFrame,
    players: pd.DataFrame | None = None,
    *,
    feature_version: str = FEATURE_VERSION,
) -> pd.DataFrame:
    hand_features = compute_player_hand_features(
        actions,
        players,
        training_dataset="session_scoring",
        feature_version=feature_version,
    )
    return aggregate_session_hand_features(
        hand_features,
        feature_version=feature_version,
    )


def aggregate_session_hand_features(
    hand_features: pd.DataFrame,
    *,
    feature_version: str = FEATURE_VERSION,
) -> pd.DataFrame:
    if hand_features.empty:
        return pd.DataFrame(columns=PROFILE_SESSION_FEATURE_COLUMNS)

    rows: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    grouped = hand_features.groupby(
        ["source_type", "source_dataset", "source_run_id", "simulation_run_id", "player_id", "agent_id"],
        dropna=False,
        sort=False,
    )
    for keys, frame in grouped:
        source_type, source_dataset, source_run_id, simulation_run_id, player_id, agent_id = (
            keys if isinstance(keys, tuple) else (keys,)
        )
        training_features = aggregate_training_features(
            frame,
            training_dataset="session_scoring",
            feature_version=feature_version,
            min_hands=MIN_SESSION_HANDS,
        )
        if training_features.empty:
            continue
        feature_row = training_features.iloc[0].to_dict()
        subject_id = str(agent_id) if pd.notna(agent_id) and agent_id != "" else str(player_id)
        row = {
            "model_run_id": None,
            "feature_version": feature_version,
            "source_type": source_type,
            "source_dataset": source_dataset,
            "source_run_id": source_run_id,
            "simulation_run_id": simulation_run_id,
            "subject_id": subject_id,
            "player_id": player_id,
            "agent_id": agent_id,
            "hands_observed": int(feature_row["hands_played"]),
            "aggressive_actions": int(feature_row["aggressive_actions"]),
            "passive_actions": int(feature_row["passive_actions"]),
            "flop_cbet_opportunities": int(feature_row["flop_cbet_opportunities"]),
            "turn_barrel_opportunities": int(feature_row["turn_barrel_opportunities"]),
            "late_position_hands": int(feature_row["late_position_hands"]),
            "blind_position_hands": int(feature_row["blind_position_hands"]),
            "short_stack_hands": int(feature_row["short_stack_hands"]),
            "deep_stack_hands": int(feature_row["deep_stack_hands"]),
            "vpip_rate": float(feature_row["vpip_rate"]),
            "pfr_rate": float(feature_row["pfr_rate"]),
            "aggression_frequency": float(feature_row["aggression_frequency"]),
            "call_preference": float(feature_row["call_preference"]),
            "flop_cbet_rate": float(feature_row["flop_cbet_rate"]),
            "turn_barrel_rate": float(feature_row["turn_barrel_rate"]),
            "river_seen_rate": float(feature_row["river_seen_rate"]),
            "all_in_rate": float(feature_row["all_in_rate"]),
            "avg_preflop_raise_bb": float(feature_row["avg_preflop_raise_bb"]),
            "avg_postflop_bet_pot_ratio": float(feature_row["avg_postflop_bet_pot_ratio"]),
            "late_position_vpip_rate": float(feature_row["late_position_vpip_rate"]),
            "blind_defend_rate": float(feature_row["blind_defend_rate"]),
            "short_stack_aggression_rate": float(feature_row["short_stack_aggression_rate"]),
            "deep_stack_looseness": float(feature_row["deep_stack_looseness"]),
            "scored_at": now,
        }
        rows.append(row)

    return pd.DataFrame(rows, columns=PROFILE_SESSION_FEATURE_COLUMNS)


def _prepare_actions(actions: pd.DataFrame) -> pd.DataFrame:
    if actions is None or actions.empty:
        return pd.DataFrame()
    frame = actions.copy()
    frame.columns = [str(column).lower() for column in frame.columns]
    for column in [
        "source_run_id",
        "source_type",
        "source_dataset",
        "simulation_run_id",
        "table_id",
        "hand_id",
        "player_id",
        "agent_id",
        "position",
        "street",
        "action_type",
    ]:
        if column not in frame.columns:
            frame[column] = None
    for column in ["action_index", "amount_bb", "pot_before_bb", "effective_stack_bb"]:
        if column not in frame.columns:
            frame[column] = 0
    frame["action_index"] = pd.to_numeric(frame["action_index"], errors="coerce").fillna(0).astype(int)
    for column in ["amount_bb", "pot_before_bb", "effective_stack_bb"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
    frame["street"] = frame["street"].fillna("").astype(str).str.lower()
    frame["action_type"] = frame["action_type"].fillna("").astype(str).str.lower()
    frame["position"] = frame["position"].fillna("UNKNOWN").astype(str).str.upper()
    if "is_all_in" not in frame.columns:
        frame["is_all_in"] = False
    frame["is_all_in"] = frame["is_all_in"].fillna(False).astype(bool)
    return frame


def _prepare_players(players: pd.DataFrame | None) -> pd.DataFrame:
    if players is None or players.empty:
        return pd.DataFrame(columns=[*ACTION_JOIN_COLUMNS, "stack_start_bb"])
    frame = players.copy()
    frame.columns = [str(column).lower() for column in frame.columns]
    for column in ACTION_JOIN_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    if "stack_start_bb" not in frame.columns:
        frame["stack_start_bb"] = None
    frame["stack_start_bb"] = pd.to_numeric(frame["stack_start_bb"], errors="coerce")
    return frame[[*ACTION_JOIN_COLUMNS, "stack_start_bb"]].drop_duplicates()


def _attach_player_stacks(actions: pd.DataFrame, players: pd.DataFrame) -> pd.DataFrame:
    if players.empty:
        enriched = actions.copy()
        enriched["stack_start_bb"] = enriched["effective_stack_bb"]
        return enriched

    enriched = _null_safe_merge(actions, players, ACTION_JOIN_COLUMNS)
    if "stack_start_bb" not in enriched.columns:
        enriched["stack_start_bb"] = None
    enriched["stack_start_bb"] = enriched["stack_start_bb"].fillna(enriched["effective_stack_bb"])
    return enriched


def _street_aggressor_map(actions: pd.DataFrame, *, street: str, last_aggressor: bool) -> dict[tuple[Any, ...], str]:
    street_actions = actions[(actions["street"] == street) & actions["action_type"].isin(AGGRESSIVE_ACTIONS)].copy()
    if street_actions.empty:
        return {}
    street_actions = street_actions.sort_values(HAND_KEY_COLUMNS + ["action_index"], ascending=True)
    reducer = "last" if last_aggressor else "first"
    grouped = street_actions.groupby(HAND_KEY_COLUMNS, dropna=False, sort=False)["player_id"]
    reduced = getattr(grouped, reducer)()
    return {_key_tuple(index if isinstance(index, tuple) else (index,)): str(player_id) for index, player_id in reduced.items()}


def _null_safe_merge(left: pd.DataFrame, right: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if right.empty:
        return left.copy()
    merged_left = left.copy()
    merged_right = right.copy()
    join_columns: list[str] = []
    for column in columns:
        join_column = f"__join_{column}"
        merged_left[join_column] = merged_left[column].astype("string").fillna("__null__")
        merged_right[join_column] = merged_right[column].astype("string").fillna("__null__")
        join_columns.append(join_column)
    merged = merged_left.merge(
        merged_right.drop(columns=columns),
        on=join_columns,
        how="left",
    )
    return merged.drop(columns=join_columns)


def _key_tuple(values: Iterable[Any]) -> tuple[Any, ...]:
    result: list[Any] = []
    for value in values:
        if pd.isna(value):
            result.append(None)
        else:
            result.append(value)
    return tuple(result)


def _first_non_null(series: pd.Series, default: Any = None) -> Any:
    for value in series:
        if pd.notna(value) and value != "":
            return value
    return default


def _smoothed_ratio(numerator: float, denominator: float, *, smooth: bool = True) -> float:
    numerator = float(numerator)
    denominator = float(denominator)
    if denominator <= 0:
        return 0.0
    if not smooth:
        return numerator / denominator
    return (numerator + 1.0) / (denominator + 2.0)
