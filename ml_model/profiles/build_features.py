from __future__ import annotations

import argparse
import json

from poker_platform.storage import bootstrap_backend
from profiles.constants import DEFAULT_TRAINING_DATASET, FEATURE_VERSION, MIN_TRAINING_HANDS
from profiles.sql import profile_player_hand_features_sql, profile_player_training_features_sql


def build_profile_feature_tables(
    *,
    training_dataset: str = DEFAULT_TRAINING_DATASET,
    feature_version: str = FEATURE_VERSION,
    source_dataset: str | None = None,
    min_hands: int = MIN_TRAINING_HANDS,
) -> dict[str, int | str]:
    warehouse = bootstrap_backend()
    warehouse.execute(
        profile_player_hand_features_sql(
            training_dataset=training_dataset,
            feature_version=feature_version,
            source_dataset=source_dataset,
        )
    )
    warehouse.execute(
        profile_player_training_features_sql(
            training_dataset=training_dataset,
            feature_version=feature_version,
            min_hands=min_hands,
        )
    )
    return {
        "training_dataset": training_dataset,
        "feature_version": feature_version,
        "profile_player_hand_features": warehouse.count_rows("PROFILE_PLAYER_HAND_FEATURES"),
        "profile_player_training_features": warehouse.count_rows("PROFILE_PLAYER_TRAINING_FEATURES"),
        "eligible_training_players": warehouse.count_rows(
            "PROFILE_PLAYER_TRAINING_FEATURES",
            filters_eq={"training_dataset": training_dataset, "feature_version": feature_version, "eligible_for_training": True},
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Snowflake-backed historical profiling feature tables.")
    parser.add_argument("--training-dataset", default=DEFAULT_TRAINING_DATASET)
    parser.add_argument("--feature-version", default=FEATURE_VERSION)
    parser.add_argument("--source-dataset", default=None)
    parser.add_argument("--min-hands", type=int, default=MIN_TRAINING_HANDS)
    args = parser.parse_args(argv)

    result = build_profile_feature_tables(
        training_dataset=args.training_dataset,
        feature_version=args.feature_version,
        source_dataset=args.source_dataset,
        min_hands=args.min_hands,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

