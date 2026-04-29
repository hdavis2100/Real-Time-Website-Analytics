from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from typing import Any

import pandas as pd

from poker_platform.config import get_config
from poker_platform.storage import bootstrap_backend, get_warehouse
from profiles.train_model import (
    MIN_ACTIVATION_TRAINING_PLAYERS,
    deactivate_active_profile_models,
    _replace_rows,
)


def activate_profile_model(
    model_run_id: str,
    *,
    warehouse: Any | None = None,
    force: bool = False,
) -> dict[str, Any]:
    warehouse = _warehouse_or_default(warehouse)
    runs = warehouse.load_table(
        "PROFILE_MODEL_RUNS",
        filters_eq={"model_run_id": model_run_id},
        limit=1,
    )
    if runs.empty:
        return {
            "status": "not_found",
            "message": f"Profile model {model_run_id} was not found",
            "model_run_id": model_run_id,
        }

    run = runs.iloc[0].to_dict()
    training_player_count = int(run.get("training_player_count") or 0)
    if not force and training_player_count < MIN_ACTIVATION_TRAINING_PLAYERS:
        return {
            "status": "rejected",
            "message": (
                f"Refusing to activate {model_run_id} because "
                f"training_player_count={training_player_count} is below "
                f"{MIN_ACTIVATION_TRAINING_PLAYERS}"
            ),
            "model_run_id": model_run_id,
            "training_player_count": training_player_count,
        }

    deactivate_active_profile_models(warehouse)
    activated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    run["active"] = True
    run["status"] = "active"
    run["activated_at"] = activated_at
    _replace_rows(warehouse, "PROFILE_MODEL_RUNS", pd.DataFrame([run]), ["model_run_id"])
    return {
        "status": "ready",
        "model_run_id": model_run_id,
        "training_dataset": run.get("training_dataset"),
        "training_player_count": training_player_count,
        "activated_at": activated_at.isoformat(),
        "force": bool(force),
    }


def _warehouse_or_default(warehouse: Any | None = None):
    if warehouse is not None:
        return warehouse
    if os.getenv("POKER_PLATFORM_SKIP_SCHEMA_BOOTSTRAP") == "1":
        return get_warehouse(get_config())
    return bootstrap_backend()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Activate an existing profile model run.")
    parser.add_argument("--model-run-id", required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)

    result = activate_profile_model(args.model_run_id, force=args.force)
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("status") == "ready" else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
