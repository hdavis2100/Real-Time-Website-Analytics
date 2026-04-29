from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import historical_ingest.handhq_backfill as handhq_backfill


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "historical" / "handhq_6max_sample.phhs"


def test_run_handhq_backfill_filters_to_strict_six_handed_docs(tmp_path) -> None:
    summary = handhq_backfill.run_handhq_backfill(
        source=FIXTURE_PATH,
        source_dataset="handhq_fixture",
        source_run_id="run_1",
        table_id="hist_6max",
        worker_count=1,
        target_rows_per_chunk=10,
        spool_dir=tmp_path / "spool",
        write=False,
    )

    assert summary["discovered_files"] == 1
    assert summary["processed_files"] == 1
    assert summary["loaded_hands"] == 2
    assert summary["rejected_hands"] == 2
    assert summary["raw_hand_rows"] == 2
    assert summary["raw_player_rows"] == 12
    assert summary["raw_action_rows"] > 0


def test_canonicalize_handhq_hand_preserves_stable_anonymized_identity() -> None:
    documents = [document for _, document in handhq_backfill.iter_handhq_documents(FIXTURE_PATH)]

    first = handhq_backfill.canonicalize_handhq_hand(
        document=documents[0],
        source_file=FIXTURE_PATH,
        source_dataset="handhq_fixture",
        source_run_id="run_1",
        table_id="hist_6max",
        document_index=0,
        file_content_hash="fixture_hash",
    )
    second = handhq_backfill.canonicalize_handhq_hand(
        document=documents[1],
        source_file=FIXTURE_PATH,
        source_dataset="handhq_fixture",
        source_run_id="run_1",
        table_id="hist_6max",
        document_index=1,
        file_content_hash="fixture_hash",
    )

    first_player_1 = next(row for row in first["players"] if row["seat"] == 6)
    second_player_1 = next(row for row in second["players"] if row["seat"] == 2)
    assert first_player_1["player_id"] == second_player_1["player_id"]
    assert first_player_1["player_name"] != "token_a"

    lineage = json.loads(first_player_1["raw_lineage"])
    assert lineage["source_player_token"] == "token_a"

    hand_lineage = json.loads(first["hands"][0]["raw_lineage"])
    assert hand_lineage["venue"] == "PokerStars"
    assert first["hands"][0]["button_seat"] == 5

    call_rows = [row for row in first["actions"] if row["action_type"] == "call"]
    assert call_rows
    assert all(row["amount_bb"] is not None for row in call_rows)
    assert call_rows[0]["amount_bb"] == 2.0


def test_process_handhq_file_writes_chunked_parquet(tmp_path) -> None:
    result = handhq_backfill._process_handhq_file(
        file_path=FIXTURE_PATH,
        source_dataset="handhq_fixture",
        source_run_id="run_1",
        table_id="hist_6max",
        target_rows_per_chunk=10,
        spool_root=tmp_path / "spool",
    )

    assert result.hands_seen == 4
    assert result.hands_loaded == 2
    assert result.hands_rejected == 2
    assert len(result.artifacts) >= 2

    first_artifact = result.artifacts[0]
    assert first_artifact.hand_file is not None
    assert result.content_hash in Path(first_artifact.hand_file).name
    hand_frame = pd.read_parquet(first_artifact.hand_file)
    assert "hand_id" in hand_frame.columns
    assert len(hand_frame) >= 1


def test_run_handhq_backfill_skips_manifest_loaded_files(monkeypatch, tmp_path) -> None:
    loaded_hash = handhq_backfill._hash_file(FIXTURE_PATH)

    class _FakeWarehouse:
        def load_table(self, table: str, **kwargs) -> pd.DataFrame:
            if table == handhq_backfill.FILES_TABLE:
                return pd.DataFrame([{"content_hash": loaded_hash}])
            return pd.DataFrame()

        def delete_matching_keys(self, table: str, frame: pd.DataFrame, key_columns: list[str]) -> None:
            return None

        def write_dataframe(self, table: str, frame: pd.DataFrame, mode: str = "append", *, force_bulk: bool = False) -> None:
            return None

    monkeypatch.setattr(handhq_backfill, "bootstrap_backend", lambda: _FakeWarehouse())

    summary = handhq_backfill.run_handhq_backfill(
        source=FIXTURE_PATH,
        source_dataset="handhq_fixture",
        source_run_id="run_1",
        table_id="hist_6max",
        worker_count=1,
        target_rows_per_chunk=10,
        spool_dir=tmp_path / "spool",
        write=True,
    )

    assert summary["skipped_files"] == 1
    assert summary["processed_files"] == 0
    assert summary["loaded_hands"] == 0


def test_raw_merge_keys_are_rerun_idempotent() -> None:
    assert handhq_backfill.KEY_COLUMNS["hands"] == ["hand_id", "source_type"]
    assert handhq_backfill.KEY_COLUMNS["players"] == ["hand_id", "player_id", "source_type"]
    assert handhq_backfill.KEY_COLUMNS["actions"] == ["hand_id", "action_index", "source_type"]


def test_hand_id_is_stable_per_document_location() -> None:
    base_document = {
        "venue": "Full Tilt Poker",
        "hand": 123,
        "table": "ftp_table_1",
        "year": 2009,
        "month": 7,
        "day": 1,
        "time": "00:00:00",
    }

    first = handhq_backfill._hand_id_for_document(
        base_document,
        source_dataset="handhq_fixture",
        document_index=0,
        file_content_hash="fixture_hash",
    )
    second = handhq_backfill._hand_id_for_document(
        base_document,
        source_dataset="handhq_fixture",
        document_index=1,
        file_content_hash="fixture_hash",
    )
    assert first != second

    stable_first = handhq_backfill._hand_id_for_document(
        base_document,
        source_dataset="handhq_fixture",
        document_index=0,
        file_content_hash="fixture_hash",
    )
    stable_second = handhq_backfill._hand_id_for_document(
        base_document,
        source_dataset="handhq_fixture",
        document_index=0,
        file_content_hash="fixture_hash",
    )
    assert stable_first == stable_second


def test_hand_id_changes_when_file_locator_changes_even_if_metadata_matches() -> None:
    base_document = {
        "venue": "iPoker Network",
        "hand": 3406959394,
        "table": "HandHQ",
        "year": 2009,
        "month": 7,
        "day": 1,
        "time": "10:06:19",
    }

    first = handhq_backfill._hand_id_for_document(
        base_document,
        source_dataset="handhq_fixture",
        document_index=744,
        file_content_hash="file_a",
    )
    second = handhq_backfill._hand_id_for_document(
        base_document,
        source_dataset="handhq_fixture",
        document_index=745,
        file_content_hash="file_a",
    )
    assert first != second
