from __future__ import annotations

FEATURE_VERSION = "historical_human_archetypes_v2"
DEFAULT_TRAINING_DATASET = "all_historical_humans"
MIN_TRAINING_HANDS = 200
MIN_SESSION_HANDS = 50
CANDIDATE_CLUSTER_COUNTS = [8, 10, 12, 14]
SIMULATED_PREFLOP_RAISE_BB_CAP = 150.0
SIMULATED_POSTFLOP_BET_POT_RATIO_CAP = 12.0

FEATURE_COLUMNS = [
    "vpip_rate",
    "pfr_rate",
    "aggression_frequency",
    "call_preference",
    "flop_cbet_rate",
    "turn_barrel_rate",
    "river_seen_rate",
    "all_in_rate",
    "avg_preflop_raise_bb",
    "avg_postflop_bet_pot_ratio",
    "late_position_vpip_rate",
    "blind_defend_rate",
    "short_stack_aggression_rate",
    "deep_stack_looseness",
]

COUNT_COLUMNS = [
    "aggressive_actions",
    "passive_actions",
    "flop_cbet_opportunities",
    "turn_barrel_opportunities",
    "late_position_hands",
    "blind_position_hands",
    "short_stack_hands",
    "deep_stack_hands",
]

PROFILE_PLAYER_HAND_FEATURE_COLUMNS = [
    "training_dataset",
    "feature_version",
    "source_type",
    "source_dataset",
    "source_run_id",
    "simulation_run_id",
    "table_id",
    "hand_id",
    "player_id",
    "agent_id",
    "position",
    "stack_start_bb",
    "preflop_vpip",
    "preflop_pfr",
    "aggressive_actions",
    "passive_actions",
    "preflop_aggressive_amount_total_bb",
    "preflop_aggressive_action_count",
    "postflop_aggressive_bet_pot_ratio_sum",
    "postflop_aggressive_bet_count",
    "saw_flop",
    "saw_turn",
    "saw_river",
    "all_in",
    "cbet_opportunity_flop",
    "flop_cbet",
    "turn_barrel_opportunity",
    "turn_barrel",
    "late_position_hand",
    "late_position_vpip",
    "blind_position_hand",
    "blind_defend",
    "short_stack_hand",
    "short_stack_aggressive_actions",
    "short_stack_passive_actions",
    "deep_stack_hand",
    "deep_stack_vpip",
    "updated_at",
]

PROFILE_PLAYER_TRAINING_FEATURE_COLUMNS = [
    "training_dataset",
    "feature_version",
    "player_id",
    "source_dataset",
    "hands_played",
    *COUNT_COLUMNS,
    *FEATURE_COLUMNS,
    "eligible_for_training",
    "updated_at",
]

PROFILE_SESSION_FEATURE_COLUMNS = [
    "model_run_id",
    "feature_version",
    "source_type",
    "source_dataset",
    "source_run_id",
    "simulation_run_id",
    "subject_id",
    "player_id",
    "agent_id",
    "hands_observed",
    *COUNT_COLUMNS,
    *FEATURE_COLUMNS,
    "scored_at",
]
