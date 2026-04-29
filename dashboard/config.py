from __future__ import annotations


DEFAULT_TABLE_LIMIT = 500

PREFERRED_METRIC_COLUMNS = (
    "vpip_rate",
    "pfr_rate",
    "aggression_frequency",
    "flop_cbet_rate",
    "all_in_rate",
    "river_seen_rate",
)

ACTION_FILTER_COLUMNS = (
    "simulation_run_id",
    "player_id",
    "agent_id",
    "street",
    "action_type",
    "source_type",
    "persona_name",
    "hand_id",
    "table_id",
)
