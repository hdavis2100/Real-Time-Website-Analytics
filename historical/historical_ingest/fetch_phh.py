from __future__ import annotations

"""Fetch and prepare PHH historical data sources.

The loader accepts either a local file/directory or a remote URL. Remote files are
cached under the platform runtime directory and common archive formats are
extracted before parsing. This keeps the PHH ingest path usable for a large
external dataset instead of only the bundled fixture.
"""

import argparse
import gzip
import json
from pathlib import Path
import shutil
import tarfile
from typing import Iterable
from urllib.parse import urlparse
import zipfile

import requests

from poker_platform.config import get_config


PHH_EXTENSIONS = {".phh", ".phhs", ".txt"}
ARCHIVE_SUFFIXES = {".zip", ".tar", ".tgz", ".gz", ".bz2", ".xz"}


def is_url(value: str | Path) -> bool:
    parsed = urlparse(str(value))
    return parsed.scheme in {"http", "https"}


def resolve_phh_source(source: str | Path, *, cache_dir: str | Path | None = None, force: bool = False) -> Path:
    """Return a local path for a PHH source.

    ``source`` may be a local PHH file, a directory containing PHH files, or an
    HTTP(S) URL pointing at a PHH file or archive. Remote downloads are cached
    and extracted once unless ``force`` is true.
    """

    source_text = str(source)
    if not is_url(source_text):
        path = Path(source_text).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"PHH source does not exist: {path}")
        return path

    config = get_config()
    root = Path(cache_dir or config.runtime_dir / "phh_cache").expanduser()
    root.mkdir(parents=True, exist_ok=True)
    downloaded = _download_to_cache(source_text, root, force=force)
    return _extract_if_archive(downloaded, root, force=force)


def discover_phh_files(path: str | Path, *, limit: int | None = None) -> list[Path]:
    path_obj = Path(path)
    if path_obj.is_file():
        return [path_obj]
    if not path_obj.is_dir():
        raise FileNotFoundError(f"PHH path does not exist: {path_obj}")
    files = [file_path for file_path in sorted(path_obj.rglob("*")) if file_path.is_file() and file_path.suffix.lower() in PHH_EXTENSIONS]
    if limit is not None:
        files = files[: max(0, int(limit))]
    return files


def iter_phh_files(path: str | Path, *, limit: int | None = None) -> Iterable[Path]:
    yield from discover_phh_files(path, limit=limit)


def _download_to_cache(url: str, cache_dir: Path, *, force: bool) -> Path:
    parsed = urlparse(url)
    filename = Path(parsed.path).name or "phh_dataset"
    target = cache_dir / filename
    if target.exists() and not force:
        return target

    temporary = target.with_suffix(target.suffix + ".part")
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with temporary.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
    temporary.replace(target)
    return target


def _extract_if_archive(path: Path, cache_dir: Path, *, force: bool) -> Path:
    lower_name = path.name.lower()
    if not any(lower_name.endswith(suffix) for suffix in ARCHIVE_SUFFIXES):
        return path

    extract_dir = cache_dir / f"{path.stem}_extracted"
    if extract_dir.exists() and not force:
        return extract_dir
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)

    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            _safe_extract_zip(archive, extract_dir)
        return extract_dir

    if tarfile.is_tarfile(path):
        with tarfile.open(path) as archive:
            _safe_extract_tar(archive, extract_dir)
        return extract_dir

    if lower_name.endswith(".gz") and not lower_name.endswith(".tar.gz") and not lower_name.endswith(".tgz"):
        target = extract_dir / path.with_suffix("").name
        with gzip.open(path, "rb") as source, target.open("wb") as destination:
            shutil.copyfileobj(source, destination)
        return extract_dir

    return path


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _safe_extract_zip(archive: zipfile.ZipFile, target_dir: Path) -> None:
    for member in archive.infolist():
        destination = target_dir / member.filename
        if not _is_relative_to(destination, target_dir):
            raise RuntimeError(f"Unsafe archive member path: {member.filename}")
    archive.extractall(target_dir)


def _safe_extract_tar(archive: tarfile.TarFile, target_dir: Path) -> None:
    for member in archive.getmembers():
        destination = target_dir / member.name
        if not _is_relative_to(destination, target_dir):
            raise RuntimeError(f"Unsafe archive member path: {member.name}")
        if member.issym() or member.islnk():
            raise RuntimeError(f"Archive links are not allowed: {member.name}")
    archive.extractall(target_dir)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download/cache a PHH source and list parseable files.")
    parser.add_argument("--source", required=True, help="Local file, directory, or HTTP(S) URL")
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args(argv)

    resolved = resolve_phh_source(args.source, cache_dir=args.cache_dir, force=args.force)
    files = discover_phh_files(resolved, limit=args.limit)
    print(
        json.dumps(
            {
                "source": args.source,
                "resolved_path": str(resolved),
                "file_count": len(files),
                "files": [str(path) for path in files[:25]],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
