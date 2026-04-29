from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from typing import Any

from poker_platform.config import get_config
from poker_platform.storage import bootstrap_backend, get_warehouse


def get_active_model_status(warehouse: Any | None = None) -> dict[str, Any]:
    warehouse = _warehouse_or_default(warehouse)
    runs = warehouse.load_table(
        "PROFILE_MODEL_RUNS",
        filters_eq={"active": True},
        order_by=[("activated_at", False), ("created_at", False)],
        limit=1,
    )
    if runs.empty:
        return {"status": "not_found", "message": "No active profile model is available"}

    run = runs.iloc[0].to_dict()
    activated_at = _format_timestamp(run.get("activated_at") or run.get("created_at"))
    return {
        "status": "ready",
        "model_run_id": str(run["model_run_id"]),
        "feature_version": run.get("feature_version"),
        "training_dataset": run.get("training_dataset"),
        "activated_at": activated_at,
    }


def _format_timestamp(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _warehouse_or_default(warehouse: Any | None = None):
    if warehouse is not None:
        return warehouse
    if os.getenv("POKER_PLATFORM_SKIP_SCHEMA_BOOTSTRAP") == "1":
        return get_warehouse(get_config())
    return bootstrap_backend()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Return the currently active profile model metadata.")
    parser.parse_args(argv)
    print(json.dumps(get_active_model_status(), indent=2, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
