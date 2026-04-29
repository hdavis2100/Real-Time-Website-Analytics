from __future__ import annotations

"""Large-scale HandHQ historical backfill into Snowflake.

This module is the supported historical ingest path for HandHQ `.phhs`
corpora, preserving stable anonymized player identity and loading through
staged Parquet plus ``COPY INTO`` / ``MERGE``.
"""

import argparse
import ast
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
from typing import Any, Iterable, Iterator

import pandas as pd

from historical_ingest.fetch_phh import discover_phh_files, resolve_phh_source
from historical_ingest.normalize import position_for_seat
from poker_platform.config import get_config
from poker_platform.schemas import ActionType, PAYLOAD_VERSION, SourceType
from poker_platform.storage import TABLE_SCHEMAS, bootstrap_backend


STAGE_NAME = "HISTORICAL_HANDHQ_STAGE"
LANDING_TABLES = {
    "hands": "HISTORICAL_HANDHQ_LANDING_HANDS",
    "players": "HISTORICAL_HANDHQ_LANDING_PLAYERS",
    "actions": "HISTORICAL_HANDHQ_LANDING_ACTIONS",
}
RAW_TABLES = {
    "hands": "RAW_HANDS",
    "players": "RAW_PLAYERS",
    "actions": "RAW_ACTIONS",
}
CURATED_TABLES = {
    "hands": "CURATED_HANDS",
    "players": "CURATED_PLAYERS",
    "actions": "CURATED_ACTIONS",
}
KEY_COLUMNS = {
    # Historical reruns may legitimately change source_run_id while replaying the
    # same stable HandHQ hand_id/player_id/action_index tuples for a dataset.
    # Keep raw-table merges idempotent across resumptions by excluding run id.
    "hands": ["hand_id", "source_type"],
    "players": ["hand_id", "player_id", "source_type"],
    "actions": ["hand_id", "action_index", "source_type"],
}
RUNS_TABLE = "HISTORICAL_HANDHQ_INGEST_RUNS"
FILES_TABLE = "HISTORICAL_HANDHQ_INGEST_FILES"
POSITION_ORDER = ["BTN", "SB", "BB", "UTG", "HJ", "CO"]
MIN_ACTION_ROWS_PER_LOAD_BATCH = 1_000_000
DEFAULT_WORKER_COUNT = max(1, min(8, max(1, (os.cpu_count() or 1) // 2)))
DEFAULT_TARGET_ROWS_PER_CHUNK = 100_000


class HandhqRejectedError(ValueError):
    """Raised when a HandHQ document is intentionally rejected."""


@dataclass(frozen=True)
class ChunkArtifact:
    chunk_id: int
    hand_file: str | None
    player_file: str | None
    action_file: str | None
    hand_rows: int
    player_rows: int
    action_rows: int


@dataclass(frozen=True)
class FileBackfillResult:
    source_file: str
    content_hash: str
    file_size_bytes: int
    hands_seen: int
    hands_loaded: int
    hands_rejected: int
    raw_hand_rows: int
    raw_player_rows: int
    raw_action_rows: int
    artifacts: list[ChunkArtifact]


def run_handhq_backfill(
    *,
    source: str | Path,
    source_dataset: str,
    source_run_id: str,
    table_id: str,
    cache_dir: str | Path | None = None,
    force_download: bool = False,
    file_limit: int | None = None,
    worker_count: int = DEFAULT_WORKER_COUNT,
    target_rows_per_chunk: int = DEFAULT_TARGET_ROWS_PER_CHUNK,
    stage_prefix: str | None = None,
    spool_dir: str | Path | None = None,
    write: bool = False,
) -> dict[str, Any]:
    config = get_config()
    resolved = resolve_phh_source(source, cache_dir=cache_dir, force=force_download)
    files = [path for path in discover_phh_files(resolved, limit=file_limit) if path.suffix.lower() == ".phhs"]
    if not files:
        raise FileNotFoundError(f"No HandHQ .phhs files found under {resolved}")

    run_started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    spool_root = Path(spool_dir or config.runtime_dir / "handhq_backfill" / source_run_id).expanduser()
    if spool_root.exists():
        shutil.rmtree(spool_root)
    spool_root.mkdir(parents=True, exist_ok=True)

    warehouse = bootstrap_backend() if write else None
    prefix = (stage_prefix or source_run_id).strip("/")
    file_hashes = {str(path): _hash_file(path) for path in files}
    existing_loaded_hashes = _loaded_hashes(warehouse, source_dataset) if warehouse else set()

    summary = {
        "source_run_id": source_run_id,
        "source_dataset": source_dataset,
        "source_uri": str(source),
        "resolved_path": str(resolved),
        "stage_name": STAGE_NAME,
        "stage_prefix": prefix,
        "status": "running" if write else "parsed",
        "started_at": run_started_at,
        "finished_at": None,
        "discovered_files": len(files),
        "skipped_files": 0,
        "processed_files": 0,
        "failed_files": 0,
        "rejected_files": 0,
        "loaded_hands": 0,
        "rejected_hands": 0,
        "raw_hand_rows": 0,
        "raw_player_rows": 0,
        "raw_action_rows": 0,
        "message": "",
    }

    if warehouse:
        _upsert_run_row(warehouse, summary)

    pending: list[Path] = []
    batch_results: list[FileBackfillResult] = []
    batch_action_rows = 0
    batch_id = 0
    load_batch_rows = max(int(target_rows_per_chunk), MIN_ACTION_ROWS_PER_LOAD_BATCH)
    for file_path in files:
        if file_hashes[str(file_path)] in existing_loaded_hashes:
            summary["skipped_files"] += 1
            continue
        pending.append(file_path)

    try:
        if worker_count <= 1:
            for file_path in pending:
                result = _process_handhq_file(
                    file_path=file_path,
                    source_dataset=source_dataset,
                    source_run_id=source_run_id,
                    table_id=table_id,
                    target_rows_per_chunk=target_rows_per_chunk,
                    spool_root=spool_root,
                )
                batch_action_rows, batch_id = _queue_processed_file(
                    warehouse=warehouse,
                    summary=summary,
                    result=result,
                    batch_results=batch_results,
                    batch_action_rows=batch_action_rows,
                    batch_id=batch_id,
                    load_batch_rows=load_batch_rows,
                    stage_prefix=prefix,
                    run_started_at=run_started_at,
                    spool_root=spool_root,
                )
        else:
            with ProcessPoolExecutor(max_workers=max(1, int(worker_count))) as executor:
                futures = {
                    executor.submit(
                        _process_handhq_file,
                        file_path=file_path,
                        source_dataset=source_dataset,
                        source_run_id=source_run_id,
                        table_id=table_id,
                        target_rows_per_chunk=target_rows_per_chunk,
                        spool_root=spool_root,
                    ): file_path
                    for file_path in pending
                }
                for future in as_completed(futures):
                    file_path = futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        summary["failed_files"] += 1
                        if warehouse:
                            _upsert_file_row(
                                warehouse,
                                {
                                    "content_hash": file_hashes[str(file_path)],
                                    "source_run_id": source_run_id,
                                    "source_file": str(file_path),
                                    "source_dataset": source_dataset,
                                    "status": "failed",
                                    "file_size_bytes": file_path.stat().st_size,
                                    "discovered_at": run_started_at,
                                    "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
                                    "hands_seen": 0,
                                    "hands_loaded": 0,
                                    "hands_rejected": 0,
                                    "raw_hand_rows": 0,
                                    "raw_player_rows": 0,
                                    "raw_action_rows": 0,
                                    "last_error": str(exc),
                                },
                            )
                            _upsert_run_row(warehouse, summary)
                        continue
                    batch_action_rows, batch_id = _queue_processed_file(
                        warehouse=warehouse,
                        summary=summary,
                        result=result,
                        batch_results=batch_results,
                        batch_action_rows=batch_action_rows,
                        batch_id=batch_id,
                        load_batch_rows=load_batch_rows,
                        stage_prefix=prefix,
                        run_started_at=run_started_at,
                        spool_root=spool_root,
                    )
        if batch_results:
            _finalize_results_batch(
                warehouse=warehouse,
                summary=summary,
                results=batch_results,
                stage_prefix=f"{prefix}/batch_{batch_id:06d}",
                run_started_at=run_started_at,
                spool_root=spool_root,
            )
    except Exception as exc:
        summary["status"] = "failed"
        summary["message"] = str(exc)
        summary["finished_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
        if warehouse:
            _upsert_run_row(warehouse, summary)
        raise

    summary["status"] = "completed_with_errors" if summary["failed_files"] else "completed"
    summary["finished_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    if warehouse and summary["processed_files"] > 0:
        _refresh_curated_tables_for_dataset(warehouse, source_dataset=source_dataset)
    if warehouse:
        _upsert_run_row(warehouse, summary)
    shutil.rmtree(spool_root, ignore_errors=True)
    return _serialize_summary(summary)


def _loaded_hashes(warehouse: Any | None, source_dataset: str) -> set[str]:
    if warehouse is None:
        return set()
    frame = warehouse.load_table(
        FILES_TABLE,
        filters_eq={"source_dataset": source_dataset, "status": "loaded"},
    )
    if frame.empty or "content_hash" not in frame.columns:
        return set()
    return set(frame["content_hash"].astype(str).tolist())


def _queue_processed_file(
    *,
    warehouse: Any | None,
    summary: dict[str, Any],
    result: FileBackfillResult,
    batch_results: list[FileBackfillResult],
    batch_action_rows: int,
    batch_id: int,
    load_batch_rows: int,
    stage_prefix: str,
    run_started_at: datetime,
    spool_root: str | Path,
) -> tuple[int, int]:
    if not warehouse or not result.artifacts:
        _finalize_results_batch(
            warehouse=warehouse,
            summary=summary,
            results=[result],
            stage_prefix=f"{stage_prefix}/batch_{batch_id:06d}",
            run_started_at=run_started_at,
            spool_root=spool_root,
        )
        return batch_action_rows, batch_id

    batch_results.append(result)
    batch_action_rows += result.raw_action_rows
    if batch_action_rows < load_batch_rows:
        return batch_action_rows, batch_id

    _finalize_results_batch(
        warehouse=warehouse,
        summary=summary,
        results=batch_results,
        stage_prefix=f"{stage_prefix}/batch_{batch_id:06d}",
        run_started_at=run_started_at,
        spool_root=spool_root,
    )
    return 0, batch_id + 1


def _finalize_results_batch(
    *,
    warehouse: Any | None,
    summary: dict[str, Any],
    results: list[FileBackfillResult],
    stage_prefix: str,
    run_started_at: datetime,
    spool_root: str | Path,
) -> None:
    if not results:
        return
    if warehouse and any(result.artifacts for result in results):
        _load_result_batch(
            warehouse=warehouse,
            results=results,
            stage_prefix=stage_prefix,
        )
    file_rows: list[dict[str, Any]] = []
    for result in results:
        summary["processed_files"] += 1
        summary["rejected_files"] += 1 if result.hands_rejected else 0
        summary["loaded_hands"] += result.hands_loaded
        summary["rejected_hands"] += result.hands_rejected
        summary["raw_hand_rows"] += result.raw_hand_rows
        summary["raw_player_rows"] += result.raw_player_rows
        summary["raw_action_rows"] += result.raw_action_rows
        if warehouse:
            file_rows.append(
                {
                    "content_hash": result.content_hash,
                    "source_run_id": summary["source_run_id"],
                    "source_file": result.source_file,
                    "source_dataset": summary["source_dataset"],
                    "status": "loaded" if result.hands_loaded else "rejected",
                    "file_size_bytes": result.file_size_bytes,
                    "discovered_at": run_started_at,
                    "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
                    "hands_seen": result.hands_seen,
                    "hands_loaded": result.hands_loaded,
                    "hands_rejected": result.hands_rejected,
                    "raw_hand_rows": result.raw_hand_rows,
                    "raw_player_rows": result.raw_player_rows,
                    "raw_action_rows": result.raw_action_rows,
                    "last_error": None,
                }
            )
        shutil.rmtree(Path(spool_root) / result.content_hash, ignore_errors=True)
    if warehouse:
        _upsert_file_rows(warehouse, file_rows)
        _upsert_run_row(warehouse, summary)
    results.clear()


def _process_handhq_file(
    *,
    file_path: str | Path,
    source_dataset: str,
    source_run_id: str,
    table_id: str,
    target_rows_per_chunk: int,
    spool_root: str | Path,
) -> FileBackfillResult:
    file_obj = Path(file_path)
    content_hash = _hash_file(file_obj)
    target_dir = Path(spool_root) / content_hash
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    writer = _ChunkedParquetWriter(target_dir=target_dir, target_rows_per_chunk=target_rows_per_chunk)
    hands_seen = 0
    hands_loaded = 0
    hands_rejected = 0
    for document_index, document in iter_handhq_documents(file_obj):
        hands_seen += 1
        try:
            rows = canonicalize_handhq_hand(
                document=document,
                source_file=file_obj,
                source_dataset=source_dataset,
                source_run_id=source_run_id,
                table_id=table_id,
                document_index=document_index,
                file_content_hash=content_hash,
            )
        except HandhqRejectedError:
            hands_rejected += 1
            continue
        writer.add_rows(rows)
        hands_loaded += 1
    artifacts = writer.flush()
    return FileBackfillResult(
        source_file=str(file_obj),
        content_hash=content_hash,
        file_size_bytes=file_obj.stat().st_size,
        hands_seen=hands_seen,
        hands_loaded=hands_loaded,
        hands_rejected=hands_rejected,
        raw_hand_rows=sum(artifact.hand_rows for artifact in artifacts),
        raw_player_rows=sum(artifact.player_rows for artifact in artifacts),
        raw_action_rows=sum(artifact.action_rows for artifact in artifacts),
        artifacts=artifacts,
    )


class _ChunkedParquetWriter:
    def __init__(self, *, target_dir: Path, target_rows_per_chunk: int) -> None:
        self.target_dir = target_dir
        self.target_rows_per_chunk = max(1, int(target_rows_per_chunk))
        self._chunk_id = 0
        self._hands: list[dict[str, Any]] = []
        self._players: list[dict[str, Any]] = []
        self._actions: list[dict[str, Any]] = []
        self._artifacts: list[ChunkArtifact] = []

    def add_rows(self, rows: dict[str, list[dict[str, Any]]]) -> None:
        self._hands.extend(rows["hands"])
        self._players.extend(rows["players"])
        self._actions.extend(rows["actions"])
        if len(self._actions) >= self.target_rows_per_chunk:
            self._flush_chunk()

    def flush(self) -> list[ChunkArtifact]:
        self._flush_chunk()
        return list(self._artifacts)

    def _flush_chunk(self) -> None:
        if not self._hands and not self._players and not self._actions:
            return
        artifact = ChunkArtifact(
            chunk_id=self._chunk_id,
            hand_file=self._write_table("hands", self._hands),
            player_file=self._write_table("players", self._players),
            action_file=self._write_table("actions", self._actions),
            hand_rows=len(self._hands),
            player_rows=len(self._players),
            action_rows=len(self._actions),
        )
        self._artifacts.append(artifact)
        self._chunk_id += 1
        self._hands = []
        self._players = []
        self._actions = []

    def _write_table(self, table_key: str, rows: list[dict[str, Any]]) -> str | None:
        if not rows:
            return None
        # Preserve a unique filename per source file so batch stage uploads
        # do not overwrite one another under a shared stage prefix.
        output_path = self.target_dir / f"{table_key}_{self.target_dir.name}_{self._chunk_id:06d}.parquet"
        frame = pd.DataFrame(rows)
        frame.to_parquet(output_path, engine="pyarrow", index=False)
        return str(output_path)


def iter_handhq_documents(path: str | Path) -> Iterator[tuple[int, dict[str, Any]]]:
    path_obj = Path(path)
    current: dict[str, Any] = {}
    index = 0
    lines = path_obj.read_text(encoding="utf-8").splitlines()
    cursor = 0
    while cursor < len(lines):
        raw_line = lines[cursor].strip()
        cursor += 1
        if not raw_line or raw_line.startswith("#"):
            continue
        if re.fullmatch(r"\[\d+\]", raw_line):
            if current:
                yield index, current
                index += 1
                current = {}
            continue
        if "=" not in raw_line:
            raise HandhqRejectedError(f"Malformed HandHQ line in {path_obj}: {raw_line}")
        key, rhs = [part.strip() for part in raw_line.split("=", 1)]
        if rhs.startswith("[") and not _is_bracket_closed(rhs):
            captured = [rhs]
            balance = _bracket_delta(rhs)
            while cursor < len(lines) and balance > 0:
                next_line = lines[cursor]
                cursor += 1
                captured.append(next_line)
                balance += _bracket_delta(next_line)
            rhs = "\n".join(captured)
        current[key] = _phh_literal_eval(rhs)
    if current:
        yield index, current


def canonicalize_handhq_hand(
    *,
    document: dict[str, Any],
    source_file: str | Path,
    source_dataset: str,
    source_run_id: str,
    table_id: str,
    document_index: int,
    file_content_hash: str,
) -> dict[str, list[dict[str, Any]]]:
    variant = str(document.get("variant", "")).strip()
    if variant != "NT":
        raise HandhqRejectedError("Only no-limit hold'em (`NT`) is accepted")

    players_raw = _require_list(document, "players")
    seats_raw = _require_list(document, "seats")
    starting_stacks_raw = _require_list(document, "starting_stacks")
    actions_raw = _require_list(document, "actions")
    seat_count = int(document.get("seat_count") or len(players_raw))
    if seat_count != 6:
        raise HandhqRejectedError("Only 6-max tables are accepted")
    if len(players_raw) != 6 or len(seats_raw) != 6 or len(starting_stacks_raw) != 6:
        raise HandhqRejectedError("Only exactly 6 occupied seats are accepted")

    seats = [int(value) for value in seats_raw]
    if sorted(seats) != [1, 2, 3, 4, 5, 6]:
        raise HandhqRejectedError("All six seat numbers 1..6 must be occupied")
    if any(not str(token).strip() for token in players_raw):
        raise HandhqRejectedError("All six anonymized player tokens must be present")

    venue = str(document.get("venue") or "unknown")
    big_blind = _infer_big_blind(document)
    antes = _coerce_float_list(document.get("antes", []), expected_length=6)
    blinds = _coerce_float_list(document.get("blinds_or_straddles", []), expected_length=6)
    button_seat = _derive_button_seat(seats, blinds)
    started_at = _document_started_at(document, document_index)
    hand_id = _hand_id_for_document(
        document,
        source_dataset=source_dataset,
        document_index=document_index,
        file_content_hash=file_content_hash,
    )

    actors: dict[str, dict[str, Any]] = {}
    player_rows: list[dict[str, Any]] = []
    for index, (source_token, seat, starting_stack, ante, blind) in enumerate(
        zip(players_raw, seats, starting_stacks_raw, antes, blinds),
        start=1,
    ):
        canonical_player_id = _canonical_player_id(
            venue=venue,
            source_dataset=source_dataset,
            source_player_token=str(source_token),
        )
        player_name = _public_player_name(canonical_player_id)
        stack_start_bb = _normalize_amount(starting_stack, big_blind)
        actor_label = f"p{index}"
        actors[actor_label] = {
            "actor_label": actor_label,
            "source_player_token": str(source_token),
            "player_id": canonical_player_id,
            "player_name": player_name,
            "seat": int(seat),
            "stack_start_bb": stack_start_bb,
            "ante_bb": _normalize_amount(ante, big_blind) or 0.0,
            "blind_bb": _normalize_amount(blind, big_blind) or 0.0,
        }
        player_rows.append(
            {
                "source_run_id": source_run_id,
                "source_type": SourceType.HISTORICAL.value,
                "source_dataset": source_dataset,
                "simulation_run_id": None,
                "table_id": table_id,
                "hand_id": hand_id,
                "player_id": canonical_player_id,
                "agent_id": None,
                "seat": int(seat),
                "player_name": player_name,
                "stack_start_bb": _finite_or_none(stack_start_bb),
                "stack_end_bb": None,
                "hole_cards": None,
                "result_bb": None,
                "persona_name": None,
                "persona_text": None,
                "payload_version": PAYLOAD_VERSION,
                "raw_lineage": _json_lineage(
                    {
                        "source_file": str(source_file),
                        "source_player_token": str(source_token),
                        "actor_label": actor_label,
                        "position": position_for_seat(int(seat), button_seat),
                    }
                ),
            }
        )

    forced_rows = _forced_action_rows(
        hand_id=hand_id,
        source_run_id=source_run_id,
        source_dataset=source_dataset,
        table_id=table_id,
        actors=actors,
        button_seat=button_seat,
        started_at=started_at,
    )
    parsed_rows, final_board = _parse_action_rows(
        hand_id=hand_id,
        source_file=source_file,
        source_dataset=source_dataset,
        source_run_id=source_run_id,
        table_id=table_id,
        started_at=started_at,
        actors=actors,
        actions_raw=actions_raw,
        big_blind=big_blind,
        starting_pot_bb=round(sum(actor["ante_bb"] + actor["blind_bb"] for actor in actors.values()), 6),
        button_seat=button_seat,
        action_index_offset=len(forced_rows),
    )
    all_action_rows = forced_rows + parsed_rows
    total_pot_bb = 0.0
    if all_action_rows:
        total_pot_bb = max(float(row["pot_after_bb"] or 0.0) for row in all_action_rows)
    finished_at = started_at + timedelta(seconds=max(1, len(all_action_rows)))

    hand_row = {
        "hand_id": hand_id,
        "source_run_id": source_run_id,
        "source_type": SourceType.HISTORICAL.value,
        "source_dataset": source_dataset,
        "simulation_run_id": None,
        "table_id": table_id,
        "button_seat": button_seat,
        "big_blind_bb": 1.0,
        "small_blind_bb": _small_blind_bb(blinds, big_blind),
        "total_pot_bb": round(total_pot_bb, 6),
        "board_cards": json.dumps(final_board),
        "started_at": started_at,
        "finished_at": finished_at,
        "payload_version": PAYLOAD_VERSION,
        "raw_lineage": _json_lineage(
            {
                "source_file": str(source_file),
                "source_hand_number": document.get("hand"),
                "table": document.get("table"),
                "venue": venue,
                "file_content_hash": file_content_hash,
            }
        ),
    }
    return {"hands": [hand_row], "players": player_rows, "actions": all_action_rows}


def _require_list(document: dict[str, Any], key: str) -> list[Any]:
    value = document.get(key)
    if not isinstance(value, list):
        raise HandhqRejectedError(f"Expected list for {key}")
    return value


def _parse_action_rows(
    *,
    hand_id: str,
    source_file: str | Path,
    source_dataset: str,
    source_run_id: str,
    table_id: str,
    started_at: datetime,
    actors: dict[str, dict[str, Any]],
    actions_raw: list[Any],
    big_blind: float,
    starting_pot_bb: float,
    button_seat: int,
    action_index_offset: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    stack_remaining: dict[str, float] = {}
    street_commitment: dict[str, float] = {}
    active_players: dict[str, bool] = {}
    for actor in actors.values():
        stack_start_bb = actor["stack_start_bb"]
        blind_bb = actor["blind_bb"]
        ante_bb = actor["ante_bb"]
        stack_remaining[actor["player_id"]] = max(0.0, stack_start_bb - blind_bb - ante_bb) if math.isfinite(stack_start_bb) else math.inf
        street_commitment[actor["player_id"]] = blind_bb
        active_players[actor["player_id"]] = True

    current_bet = max(street_commitment.values(), default=0.0)
    current_pot = starting_pot_bb
    current_street = "preflop"
    board_cards: tuple[str, ...] = ()
    rows: list[dict[str, Any]] = []

    for raw_action_index, raw_action in enumerate(actions_raw, start=1):
        if not isinstance(raw_action, str):
            raise HandhqRejectedError("Action entries must be strings")
        tokens = raw_action.split()
        if not tokens:
            continue
        if tokens[0] == "d" and len(tokens) >= 3 and tokens[1] == "dh":
            continue
        if tokens[0] == "d" and len(tokens) >= 3 and tokens[1] == "db":
            board_cards = _append_board_cards(board_cards, _split_cards(tokens[2:]))
            current_street = _street_from_board(board_cards)
            current_bet = 0.0
            street_commitment = {actor["player_id"]: 0.0 for actor in actors.values()}
            continue

        actor = actors.get(tokens[0])
        if actor is None:
            raise HandhqRejectedError(f"Unknown actor token: {tokens[0]}")
        action_code = tokens[1] if len(tokens) > 1 else ""
        amount = _coerce_action_amount(tokens[2]) if len(tokens) > 2 and _looks_numeric(tokens[2]) else None
        player_id = actor["player_id"]
        to_call = max(0.0, current_bet - street_commitment[player_id])
        delta_bb = 0.0
        is_all_in = False
        action_type = _map_action_type(action_code, to_call_bb=to_call, current_bet_bb=current_bet)
        hole_cards: tuple[str, ...] = tuple()
        amount_bb = _normalize_amount(amount, big_blind) if amount is not None else None

        if action_code == "f":
            active_players[player_id] = False
        elif action_code == "cc":
            delta_bb = min(to_call, stack_remaining[player_id])
            action_type = ActionType.CHECK.value if delta_bb == 0 else ActionType.CALL.value
            amount_bb = round(delta_bb, 6) if delta_bb > 0 else 0.0
            if stack_remaining[player_id] > 0 and delta_bb >= stack_remaining[player_id]:
                is_all_in = True
            street_commitment[player_id] += delta_bb
            stack_remaining[player_id] -= delta_bb
            current_pot += delta_bb
        elif action_code == "cbr":
            if amount is None:
                raise HandhqRejectedError(f"cbr action missing amount: {raw_action}")
            target_bb = _normalize_amount(amount, big_blind) or 0.0
            delta_bb = max(0.0, target_bb - street_commitment[player_id])
            action_type = ActionType.BET.value if current_bet <= 0.0 else ActionType.RAISE.value
            if stack_remaining[player_id] > 0 and delta_bb >= stack_remaining[player_id]:
                is_all_in = True
                delta_bb = stack_remaining[player_id]
                target_bb = street_commitment[player_id] + delta_bb
            street_commitment[player_id] = target_bb
            stack_remaining[player_id] -= delta_bb
            current_pot += delta_bb
            current_bet = max(current_bet, target_bb)
        elif action_code == "sm":
            action_type = ActionType.SHOW.value
            hole_cards = _split_cards(tokens[2:])
        elif action_code == "sd":
            action_type = ActionType.MUCK.value

        effective_stack = stack_remaining[player_id] + street_commitment.get(player_id, 0.0)
        row_index = action_index_offset + len(rows)
        rows.append(
            {
                "hand_id": hand_id,
                "action_index": row_index,
                "source_run_id": source_run_id,
                "source_type": SourceType.HISTORICAL.value,
                "source_dataset": source_dataset,
                "simulation_run_id": None,
                "table_id": table_id,
                "street": current_street,
                "player_id": player_id,
                "agent_id": None,
                "seat": actor["seat"],
                "position": position_for_seat(actor["seat"], button_seat),
                "action_type": action_type,
                "amount_bb": _finite_or_none(amount_bb),
                "pot_before_bb": round(current_pot - delta_bb, 6),
                "pot_after_bb": round(current_pot, 6),
                "to_call_bb": round(to_call, 6),
                "effective_stack_bb": _finite_or_none(effective_stack),
                "players_remaining": _active_player_count(active_players),
                "board_cards_visible": json.dumps(list(board_cards)),
                "hole_cards_visible": json.dumps(list(hole_cards)),
                "is_all_in": bool(is_all_in),
                "event_ts": started_at + timedelta(seconds=row_index),
                "backend_type": "historical_phh",
                "persona_name": None,
                "persona_text": None,
                "payload_version": PAYLOAD_VERSION,
                "raw_lineage": _json_lineage(
                    {
                        "source_file": str(source_file),
                        "raw_text": raw_action,
                        "action_code": action_code,
                        "actor_label": actor["actor_label"],
                        "source_player_token": actor["source_player_token"],
                        "original_action_index": raw_action_index,
                    }
                ),
            }
        )
    return rows, list(board_cards)


def _forced_action_rows(
    *,
    hand_id: str,
    source_run_id: str,
    source_dataset: str,
    table_id: str,
    actors: dict[str, dict[str, Any]],
    button_seat: int,
    started_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    current_pot = 0.0
    for actor in actors.values():
        if actor["ante_bb"] > 0:
            rows.append(
                _forced_action_row(
                    hand_id=hand_id,
                    source_run_id=source_run_id,
                    source_dataset=source_dataset,
                    table_id=table_id,
                    action_index=len(rows),
                    seat=actor["seat"],
                    player_id=actor["player_id"],
                    position=position_for_seat(actor["seat"], button_seat),
                    action_type=ActionType.POST_ANTE.value,
                    amount_bb=actor["ante_bb"],
                    pot_before_bb=current_pot,
                    pot_after_bb=current_pot + actor["ante_bb"],
                    to_call_bb=0.0,
                    started_at=started_at,
                    source_player_token=actor["source_player_token"],
                    actor_label=actor["actor_label"],
                )
            )
            current_pot += actor["ante_bb"]
    for actor in actors.values():
        if actor["blind_bb"] <= 0:
            continue
        action_type = ActionType.POST_SMALL_BLIND.value if actor["blind_bb"] < 1.0 else ActionType.POST_BIG_BLIND.value
        to_call_bb = max(0.0, 1.0 - actor["blind_bb"]) if action_type == ActionType.POST_SMALL_BLIND.value else 0.0
        rows.append(
            _forced_action_row(
                hand_id=hand_id,
                source_run_id=source_run_id,
                source_dataset=source_dataset,
                table_id=table_id,
                action_index=len(rows),
                seat=actor["seat"],
                player_id=actor["player_id"],
                position=position_for_seat(actor["seat"], button_seat),
                action_type=action_type,
                amount_bb=actor["blind_bb"],
                pot_before_bb=current_pot,
                pot_after_bb=current_pot + actor["blind_bb"],
                to_call_bb=to_call_bb,
                started_at=started_at,
                source_player_token=actor["source_player_token"],
                actor_label=actor["actor_label"],
            )
        )
        current_pot += actor["blind_bb"]
    return rows


def _forced_action_row(
    *,
    hand_id: str,
    source_run_id: str,
    source_dataset: str,
    table_id: str,
    action_index: int,
    seat: int,
    player_id: str,
    position: str,
    action_type: str,
    amount_bb: float,
    pot_before_bb: float,
    pot_after_bb: float,
    to_call_bb: float,
    started_at: datetime,
    source_player_token: str,
    actor_label: str,
) -> dict[str, Any]:
    return {
        "hand_id": hand_id,
        "action_index": action_index,
        "source_run_id": source_run_id,
        "source_type": SourceType.HISTORICAL.value,
        "source_dataset": source_dataset,
        "simulation_run_id": None,
        "table_id": table_id,
        "street": "preflop",
        "player_id": player_id,
        "agent_id": None,
        "seat": seat,
        "position": position,
        "action_type": action_type,
        "amount_bb": round(amount_bb, 6),
        "pot_before_bb": round(pot_before_bb, 6),
        "pot_after_bb": round(pot_after_bb, 6),
        "to_call_bb": round(to_call_bb, 6),
        "effective_stack_bb": None,
        "players_remaining": 6,
        "board_cards_visible": json.dumps([]),
        "hole_cards_visible": json.dumps([]),
        "is_all_in": False,
        "event_ts": started_at + timedelta(seconds=action_index),
        "backend_type": "historical_phh",
        "persona_name": None,
        "persona_text": None,
        "payload_version": PAYLOAD_VERSION,
        "raw_lineage": _json_lineage(
            {
                "forced_action": True,
                "source_player_token": source_player_token,
                "actor_label": actor_label,
            }
        ),
    }


def _load_result_batch(*, warehouse: Any, results: list[FileBackfillResult], stage_prefix: str) -> None:
    hand_files = [
        artifact.hand_file
        for result in results
        for artifact in result.artifacts
        if artifact.hand_file
    ]
    player_files = [
        artifact.player_file
        for result in results
        for artifact in result.artifacts
        if artifact.player_file
    ]
    action_files = [
        artifact.action_file
        for result in results
        for artifact in result.artifacts
        if artifact.action_file
    ]
    if hand_files:
        _load_table_batch(
            warehouse=warehouse,
            local_files=hand_files,
            stage_prefix=f"{stage_prefix}/hands",
            landing_table=LANDING_TABLES["hands"],
            raw_table=RAW_TABLES["hands"],
            key_columns=KEY_COLUMNS["hands"],
        )
    if player_files:
        _load_table_batch(
            warehouse=warehouse,
            local_files=player_files,
            stage_prefix=f"{stage_prefix}/players",
            landing_table=LANDING_TABLES["players"],
            raw_table=RAW_TABLES["players"],
            key_columns=KEY_COLUMNS["players"],
        )
    if action_files:
        _load_table_batch(
            warehouse=warehouse,
            local_files=action_files,
            stage_prefix=f"{stage_prefix}/actions",
            landing_table=LANDING_TABLES["actions"],
            raw_table=RAW_TABLES["actions"],
            key_columns=KEY_COLUMNS["actions"],
        )


def _load_table_batch(
    *,
    warehouse: Any,
    local_files: list[str],
    stage_prefix: str,
    landing_table: str,
    raw_table: str,
    key_columns: list[str],
) -> None:
    for local_file in local_files:
        warehouse.put_file_to_stage(STAGE_NAME, local_file, stage_prefix=stage_prefix, auto_compress=False)
    warehouse.copy_stage_into_table(
        STAGE_NAME,
        landing_table,
        stage_prefix=stage_prefix,
        purge=True,
    )
    warehouse.merge_table_into_table(raw_table, landing_table, key_columns=key_columns)
    warehouse.truncate_table(landing_table)


def _merge_query_from_raw(*, raw_table: str, landing_table: str, key_columns: list[str]) -> str:
    columns = ", ".join(f"raw.{column.upper()} AS {column.upper()}" for column in TABLE_SCHEMAS[raw_table])
    join_clause = " AND ".join(
        f"raw.{column.upper()} = landing.{column.upper()}"
        for column in key_columns
    )
    return f"SELECT {columns} FROM {raw_table} raw INNER JOIN {landing_table} landing ON {join_clause}"


def _upsert_run_row(warehouse: Any, summary: dict[str, Any]) -> None:
    frame = pd.DataFrame(
        [
            {
                "source_run_id": summary["source_run_id"],
                "source_uri": summary["source_uri"],
                "source_dataset": summary["source_dataset"],
                "stage_name": summary["stage_name"],
                "stage_prefix": summary["stage_prefix"],
                "status": summary["status"],
                "started_at": summary["started_at"],
                "finished_at": summary["finished_at"],
                "discovered_files": summary["discovered_files"],
                "skipped_files": summary["skipped_files"],
                "processed_files": summary["processed_files"],
                "failed_files": summary["failed_files"],
                "rejected_files": summary["rejected_files"],
                "loaded_hands": summary["loaded_hands"],
                "rejected_hands": summary["rejected_hands"],
                "raw_hand_rows": summary["raw_hand_rows"],
                "raw_player_rows": summary["raw_player_rows"],
                "raw_action_rows": summary["raw_action_rows"],
                "message": summary["message"],
            }
        ]
    )
    warehouse.delete_matching_keys(RUNS_TABLE, frame, ["source_run_id"])
    warehouse.write_dataframe(RUNS_TABLE, frame, force_bulk=True)


def _upsert_file_row(warehouse: Any, row: dict[str, Any]) -> None:
    frame = pd.DataFrame([row])
    warehouse.delete_matching_keys(FILES_TABLE, frame, ["content_hash", "source_dataset"])
    warehouse.write_dataframe(FILES_TABLE, frame, force_bulk=True)


def _upsert_file_rows(warehouse: Any, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    frame = pd.DataFrame(rows)
    warehouse.delete_matching_keys(FILES_TABLE, frame, ["content_hash", "source_dataset"])
    warehouse.write_dataframe(FILES_TABLE, frame, force_bulk=True)


def _refresh_curated_tables_for_dataset(warehouse: Any, *, source_dataset: str) -> None:
    escaped_dataset = source_dataset.replace("'", "''")
    escaped_source_type = SourceType.HISTORICAL.value.replace("'", "''")
    for key, raw_table in RAW_TABLES.items():
        curated_table = CURATED_TABLES[key]
        columns = ", ".join(column.upper() for column in TABLE_SCHEMAS[raw_table])
        warehouse.delete_rows(
            curated_table,
            filters_eq={
                "source_dataset": source_dataset,
                "source_type": SourceType.HISTORICAL.value,
            },
        )
        warehouse.execute(
            f"INSERT INTO {curated_table} ({columns}) "
            f"SELECT {columns} FROM {raw_table} "
            f"WHERE SOURCE_DATASET = '{escaped_dataset}' "
            f"AND SOURCE_TYPE = '{escaped_source_type}'"
        )


def _document_started_at(document: dict[str, Any], document_index: int) -> datetime:
    try:
        year = int(document.get("year"))
        month = int(document.get("month"))
        day = int(document.get("day"))
        hour, minute, second = [int(part) for part in str(document.get("time") or "00:00:00").split(":")]
        return datetime(year, month, day, hour, minute, second)
    except Exception:
        return datetime(2026, 1, 1) + timedelta(seconds=document_index)


def _hand_id_for_document(
    document: dict[str, Any],
    *,
    source_dataset: str,
    document_index: int,
    file_content_hash: str,
) -> str:
    stable_source = {
        "dataset": source_dataset,
        "venue": document.get("venue"),
        "hand": document.get("hand"),
        "table": document.get("table"),
        "year": document.get("year"),
        "month": document.get("month"),
        "day": document.get("day"),
        "time": document.get("time"),
        # Some corpora reuse hand metadata for multiple distinct documents.
        # Anchor hand identity to the stable location within the source file so
        # each HandHQ document maps to exactly one raw hand row across reruns.
        "file_content_hash": file_content_hash,
        "document_index": int(document_index),
    }
    return hashlib.sha1(json.dumps(stable_source, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _canonical_player_id(*, venue: str, source_dataset: str, source_player_token: str) -> str:
    source = f"{venue}|{source_dataset}|{source_player_token}"
    return hashlib.sha1(source.encode("utf-8")).hexdigest()


def _public_player_name(player_id: str) -> str:
    return f"anon_{player_id[:12]}"


def _hash_file(path: str | Path) -> str:
    digest = hashlib.sha1()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _infer_big_blind(document: dict[str, Any]) -> float:
    min_bet = document.get("min_bet")
    if min_bet is not None:
        parsed = _coerce_number(min_bet)
        if parsed is not None and parsed > 0:
            return float(parsed)
    blinds = _coerce_float_list(document.get("blinds_or_straddles", []))
    positive = [value for value in blinds if value > 0]
    if positive:
        return max(positive)
    return 1.0


def _derive_button_seat(seats: list[int], blinds_or_straddles: list[float]) -> int:
    positives = [(amount, seats[index]) for index, amount in enumerate(blinds_or_straddles[: len(seats)]) if amount > 0]
    if len(positives) < 2:
        raise HandhqRejectedError("Unable to derive button seat from blind postings")
    small_blind_seat = min(positives, key=lambda item: item[0])[1]
    ordered_seats = sorted(seats)
    return ordered_seats[ordered_seats.index(small_blind_seat) - 1]


def _small_blind_bb(blinds_or_straddles: Iterable[float], big_blind: float) -> float:
    positive = sorted(
        _normalize_amount(value, big_blind) or 0.0
        for value in blinds_or_straddles
        if _coerce_number(value) and _coerce_number(value) > 0
    )
    if len(positive) >= 2:
        return round(positive[0], 6)
    if positive:
        return round(min(0.5, positive[0]), 6)
    return 0.5


def _coerce_action_amount(value: Any) -> float | None:
    parsed = _coerce_number(value)
    return float(parsed) if parsed is not None else None


def _looks_numeric(value: str) -> bool:
    return bool(re.fullmatch(r"-?\d+(?:\.\d+)?", value))


def _append_board_cards(board_cards: tuple[str, ...], new_cards: tuple[str, ...]) -> tuple[str, ...]:
    return board_cards + new_cards


def _street_from_board(board_cards: tuple[str, ...]) -> str:
    if len(board_cards) == 3:
        return "flop"
    if len(board_cards) == 4:
        return "turn"
    if len(board_cards) >= 5:
        return "river"
    return "preflop"


def _map_action_type(action_code: str, *, to_call_bb: float = 0.0, current_bet_bb: float = 0.0) -> str:
    if action_code == "f":
        return ActionType.FOLD.value
    if action_code == "cc":
        return ActionType.CALL.value if to_call_bb > 0 else ActionType.CHECK.value
    if action_code == "cbr":
        return ActionType.RAISE.value if current_bet_bb > 0 else ActionType.BET.value
    if action_code == "sm":
        return ActionType.SHOW.value
    if action_code == "sd":
        return ActionType.MUCK.value
    return ActionType.CHECK.value


def _active_player_count(active_players: dict[str, bool]) -> int:
    return sum(1 for value in active_players.values() if value)


def _split_cards(tokens: list[str]) -> tuple[str, ...]:
    cards: list[str] = []
    for token in tokens:
        if not token:
            continue
        if token == "????":
            cards.extend(["??", "??"])
            continue
        if len(token) % 2 == 0 and len(token) > 2:
            cards.extend(token[index : index + 2] for index in range(0, len(token), 2))
        else:
            cards.append(token)
    return tuple(cards)


def _normalize_amount(amount: Any, big_blind: float) -> float | None:
    parsed = _coerce_number(amount)
    if parsed is None:
        return None
    if big_blind <= 0:
        return float(parsed)
    return round(float(parsed) / float(big_blind), 6)


def _finite_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    numeric = float(value)
    if not math.isfinite(numeric):
        return None
    return round(numeric, 6)


def _coerce_float_list(value: Any, *, expected_length: int | None = None) -> list[float]:
    if value is None:
        values: list[Any] = []
    elif isinstance(value, list):
        values = list(value)
    else:
        raise HandhqRejectedError(f"Expected list, got {type(value)!r}")
    if expected_length is not None and len(values) < expected_length:
        values.extend([0.0] * (expected_length - len(values)))
    result: list[float] = []
    for item in values:
        parsed = _coerce_number(item)
        result.append(float(parsed or 0.0))
    return result


def _coerce_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"inf", "+inf", "infinity", "+infinity"}:
        return float("inf")
    if lowered in {"-inf", "-infinity"}:
        return float("-inf")
    try:
        return float(text)
    except ValueError as exc:
        raise HandhqRejectedError(f"Cannot coerce value to float: {value!r}") from exc


def _json_lineage(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, default=str)


def _serialize_summary(summary: dict[str, Any]) -> dict[str, Any]:
    serialized = dict(summary)
    for key in ("started_at", "finished_at"):
        if serialized.get(key) is not None:
            serialized[key] = serialized[key].isoformat()
    return serialized


def _phh_literal_eval(value: str) -> Any:
    stripped = value.strip()
    if re.fullmatch(r"\d{2}:\d{2}:\d{2}", stripped):
        return stripped
    if stripped == "true":
        return True
    if stripped == "false":
        return False
    try:
        return ast.literal_eval(stripped)
    except Exception:
        try:
            return eval(  # noqa: S307
                stripped,
                {"__builtins__": {}},
                {
                    "inf": float("inf"),
                    "nan": float("nan"),
                    "true": True,
                    "false": False,
                },
            )
        except Exception as exc:  # pragma: no cover - sanity guard
            raise HandhqRejectedError(f"Unable to parse PHH value: {value}") from exc


def _is_bracket_closed(text: str) -> bool:
    return _bracket_delta(text) <= 0 and text.rstrip().endswith("]")


def _bracket_delta(text: str) -> int:
    delta = 0
    in_string = False
    quote = ""
    escape = False
    for char in text:
        if escape:
            escape = False
            continue
        if char == "\\":
            escape = True
            continue
        if in_string:
            if char == quote:
                in_string = False
            continue
        if char in ("'", '"'):
            in_string = True
            quote = char
            continue
        if char == "[":
            delta += 1
        elif char == "]":
            delta -= 1
    return delta


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bulk-load HandHQ 6-max NLH PHH data into Snowflake.")
    parser.add_argument("--input", required=True, help="Local HandHQ file, directory, archive, or HTTP(S) URL")
    parser.add_argument("--dataset", default="handhq_historical", help="Source dataset label")
    parser.add_argument("--source-run-id", default="historical_handhq_001", help="Warehouse lineage id")
    parser.add_argument("--table-id", default="historical_phh_6max", help="Logical table id")
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument("--force-download", action="store_true")
    parser.add_argument("--file-limit", type=int, default=None)
    parser.add_argument("--worker-count", type=int, default=DEFAULT_WORKER_COUNT)
    parser.add_argument("--target-rows-per-chunk", type=int, default=DEFAULT_TARGET_ROWS_PER_CHUNK)
    parser.add_argument("--stage-prefix", default=None)
    parser.add_argument("--spool-dir", default=None)
    parser.add_argument("--write", action="store_true", help="Write to Snowflake instead of only summarizing")
    args = parser.parse_args(argv)

    summary = run_handhq_backfill(
        source=args.input,
        source_dataset=args.dataset,
        source_run_id=args.source_run_id,
        table_id=args.table_id,
        cache_dir=args.cache_dir,
        force_download=args.force_download,
        file_limit=args.file_limit,
        worker_count=args.worker_count,
        target_rows_per_chunk=args.target_rows_per_chunk,
        stage_prefix=args.stage_prefix,
        spool_dir=args.spool_dir,
        write=args.write,
    )
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
