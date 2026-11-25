"""Resumable low-concurrency HTTP downloader with retries, backoff, and SHA-256/MD5 checks."""

from __future__ import annotations

import hashlib
import http.client
import math
import ntpath
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ie212.ingestion.schemas import BronzeError

TRANSIENT_HTTP_STATUS_CODES: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504})
_CONTENT_RANGE_RE = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$", re.IGNORECASE)
_CONTENT_RANGE_416_RE = re.compile(r"^bytes\s+\*/(\d+)$", re.IGNORECASE)
_TRUE_BOOL_STRINGS: frozenset[str] = frozenset({"true", "1", "yes", "on"})
_FALSE_BOOL_STRINGS: frozenset[str] = frozenset({"false", "0", "no", "off"})


def _coerce_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TRUE_BOOL_STRINGS:
            return True
        if normalized in _FALSE_BOOL_STRINGS:
            return False
    raise ValueError(
        f"Invalid boolean value for {field_name}: {value!r} "
        "(expected bool or true/false, 1/0, yes/no, on/off)."
    )


def _coerce_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer, got bool {value!r}.")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        try:
            return int(stripped)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an integer, got {value!r}.") from exc
    raise ValueError(f"{field_name} must be an integer, got {value!r}.")


def _coerce_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a finite number, got bool {value!r}.")
    if isinstance(value, (int, float)):
        parsed = float(value)
    elif isinstance(value, str):
        try:
            parsed = float(value.strip())
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a finite number, got {value!r}.") from exc
    else:
        raise ValueError(f"{field_name} must be a finite number, got {value!r}.")
    if not math.isfinite(parsed):
        raise ValueError(f"{field_name} must be finite, got {value!r}.")
    return parsed


def _get_header_ci(headers: Mapping[str, Any] | None, name: str) -> str | None:
    if headers is None:
        return None
    direct = headers.get(name)
    if direct is not None:
        return str(direct)
    target = name.lower()
    for key, value in headers.items():
        if str(key).lower() == target and value is not None:
            return str(value)
    return None


def _split_header_params(header_value: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    escaped = False
    for ch in header_value:
        if escaped:
            current.append(ch)
            escaped = False
            continue
        if ch == "\\" and in_quotes:
            current.append(ch)
            escaped = True
            continue
        if ch == '"':
            in_quotes = not in_quotes
            current.append(ch)
            continue
        if ch == ";" and not in_quotes:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(ch)
    parts.append("".join(current).strip())
    return [part for part in parts if part]


def _unquote_header_value(raw: str) -> str:
    val = raw.strip()
    if len(val) >= 2 and val[0] == '"' and val[-1] == '"':
        inner = val[1:-1]
        return inner.replace('\\"', '"').replace("\\\\", "\\")
    if len(val) >= 2 and val[0] == "'" and val[-1] == "'":
        return val[1:-1]
    return val


def _sanitize_cd_filename(raw: str | None) -> str | None:
    if not raw or not isinstance(raw, str):
        return None
    if "\x00" in raw or "\r" in raw or "\n" in raw:
        return None
    cleaned = raw.strip()
    if not cleaned:
        return None
    parts = [part for part in re.split(r"[\\/]+", cleaned) if part]
    if not parts or any(part == ".." for part in parts):
        return None
    _, basename = ntpath.splitdrive(parts[-1])
    basename = basename.strip()
    if not basename or basename in {".", ".."}:
        return None
    return basename


def parse_content_disposition_filename(header_value: str | None) -> str | None:
    """Extract a safe basename from a Content-Disposition header (supports RFC 5987)."""
    if not header_value or not isinstance(header_value, str):
        return None

    filename_star: str | None = None
    filename_plain: str | None = None

    for param in _split_header_params(header_value):
        if "=" not in param:
            continue
        key, raw_val = param.split("=", 1)
        norm_key = key.strip().lower()
        if norm_key == "filename*":
            unquoted = _unquote_header_value(raw_val)
            if "'" in unquoted:
                pieces = unquoted.split("'", 2)
                if len(pieces) == 3:
                    charset, _lang, encoded = pieces
                    filename_star = urllib.parse.unquote(
                        encoded,
                        encoding=(charset.strip() or "utf-8"),
                        errors="replace",
                    )
                else:
                    filename_star = urllib.parse.unquote(unquoted)
            else:
                filename_star = urllib.parse.unquote(unquoted)
        elif norm_key == "filename":
            unquoted = _unquote_header_value(raw_val)
            filename_plain = urllib.parse.unquote(unquoted)

    if filename_star is not None:
        return _sanitize_cd_filename(filename_star)
    if filename_plain is not None:
        return _sanitize_cd_filename(filename_plain)
    return None


class DownloadError(BronzeError):
    """Raised when downloading an input archive fails after retries."""


class ChecksumMismatchError(BronzeError, ValueError):
    """Raised when actual SHA-256 or MD5 does not match the expected checksum."""


@dataclass(frozen=True)
class ChecksumResult:
    """Streaming SHA-256, MD5, and byte size for a local file."""

    sha256: str
    md5: str
    size_bytes: int


@dataclass(frozen=True)
class DownloadConfig:
    """Configuration for bounded low-concurrency resumable downloads."""

    max_concurrency: int = 2
    retries: int = 5
    backoff_seconds: float = 2.0
    backoff_multiplier: float = 2.0
    timeout_seconds: float = 60.0
    chunk_size_bytes: int = 1048576
    resume: bool = True
    cleanup_temp_archives: bool = True
    verify_checksums: bool = True
    checksum_algorithms: tuple[str, ...] = ("sha256", "md5")

    def __post_init__(self) -> None:
        validate_concurrency(self.max_concurrency)
        if isinstance(self.retries, bool) or not isinstance(self.retries, int) or self.retries < 0:
            raise ValueError(f"retries must be an integer >= 0, got {self.retries!r}.")
        if isinstance(self.backoff_seconds, bool) or self.backoff_seconds < 0:
            raise ValueError(f"backoff_seconds must be >= 0, got {self.backoff_seconds!r}.")
        if isinstance(self.backoff_multiplier, bool) or self.backoff_multiplier < 1.0:
            raise ValueError(f"backoff_multiplier must be >= 1.0, got {self.backoff_multiplier!r}.")
        if isinstance(self.timeout_seconds, bool) or self.timeout_seconds <= 0:
            raise ValueError(f"timeout_seconds must be > 0, got {self.timeout_seconds!r}.")
        if (
            isinstance(self.chunk_size_bytes, bool)
            or not isinstance(self.chunk_size_bytes, int)
            or self.chunk_size_bytes <= 0
        ):
            raise ValueError(f"chunk_size_bytes must be an integer > 0, got {self.chunk_size_bytes!r}.")

    @classmethod
    def from_data_config(cls, data_cfg: Mapping[str, Any] | None) -> DownloadConfig:
        if not isinstance(data_cfg, Mapping) or not isinstance(data_cfg.get("download"), Mapping):
            return cls()
        dl = data_cfg["download"]
        algos = dl.get("checksum_algorithms", ("sha256", "md5"))
        if isinstance(algos, (str, bytes)) or not isinstance(algos, Sequence):
            raise ValueError(
                f"checksum_algorithms must be a sequence of algorithm names, got {algos!r}."
            )
        return cls(
            max_concurrency=_coerce_int(dl.get("max_concurrency", 2), "max_concurrency"),
            retries=_coerce_int(dl.get("retries", 5), "retries"),
            backoff_seconds=_coerce_float(dl.get("backoff_seconds", 2.0), "backoff_seconds"),
            backoff_multiplier=_coerce_float(
                dl.get("backoff_multiplier", 2.0), "backoff_multiplier"
            ),
            timeout_seconds=_coerce_float(dl.get("timeout_seconds", 60.0), "timeout_seconds"),
            chunk_size_bytes=_coerce_int(dl.get("chunk_size_bytes", 1048576), "chunk_size_bytes"),
            resume=_coerce_bool(dl.get("resume", True), "resume"),
            cleanup_temp_archives=_coerce_bool(
                dl.get("cleanup_temp_archives", True), "cleanup_temp_archives"
            ),
            verify_checksums=_coerce_bool(dl.get("verify_checksums", True), "verify_checksums"),
            checksum_algorithms=tuple(str(a).lower() for a in algos),
        )


@dataclass(frozen=True)
class DownloadTask:
    """Single URL download specification."""

    url: str
    dest_path: Path
    expected_sha256: str | None = None
    expected_md5: str | None = None
    prefer_content_disposition: bool = False


@dataclass(frozen=True)
class DownloadResult:
    """Completed download metadata and checksums."""

    url: str
    dest_path: Path
    sha256: str
    md5: str
    size_bytes: int
    bytes_downloaded: int
    resumed: bool
    attempts: int
    content_disposition_filename: str | None = None


def validate_concurrency(concurrency: int, max_allowed: int = 2) -> int:
    """Strictly enforce low-concurrency bounds (1..2)."""
    if isinstance(concurrency, bool) or not isinstance(concurrency, int):
        raise ValueError(f"concurrency must be an integer, got {concurrency!r}.")
    if concurrency < 1 or concurrency > 2 or concurrency > max_allowed:
        raise ValueError(
            f"concurrency must be within 1..{min(2, max_allowed)} for low-concurrency streaming, "
            f"got {concurrency}."
        )
    return concurrency


def compute_file_checksums(path: str | Path, chunk_size: int = 1048576) -> ChecksumResult:
    """Compute SHA-256 and MD5 checksums via streaming reads."""
    target = Path(path)
    if not target.is_file():
        raise FileNotFoundError(f"Cannot compute checksums; file not found: {target}")

    sha256_hash = hashlib.sha256()
    md5_hash = hashlib.md5()
    total_bytes = 0
    with target.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            sha256_hash.update(chunk)
            md5_hash.update(chunk)
            total_bytes += len(chunk)

    return ChecksumResult(
        sha256=sha256_hash.hexdigest(),
        md5=md5_hash.hexdigest(),
        size_bytes=total_bytes,
    )


def _verify_checksum_values(
    actual: ChecksumResult,
    path: str | Path,
    *,
    expected_sha256: str | None = None,
    expected_md5: str | None = None,
) -> ChecksumResult:
    if expected_sha256 is not None and actual.sha256.lower() != expected_sha256.strip().lower():
        raise ChecksumMismatchError(
            f"SHA-256 mismatch for {path}: expected {expected_sha256.strip().lower()}, "
            f"got {actual.sha256}."
        )
    if expected_md5 is not None and actual.md5.lower() != expected_md5.strip().lower():
        raise ChecksumMismatchError(
            f"MD5 mismatch for {path}: expected {expected_md5.strip().lower()}, got {actual.md5}."
        )
    return actual


def verify_file_checksums(
    path: str | Path,
    *,
    expected_sha256: str | None = None,
    expected_md5: str | None = None,
    chunk_size: int = 1048576,
) -> ChecksumResult:
    """Compute file checksums and raise ChecksumMismatchError on expected mismatch."""
    actual = compute_file_checksums(path, chunk_size=chunk_size)
    return _verify_checksum_values(
        actual,
        path,
        expected_sha256=expected_sha256,
        expected_md5=expected_md5,
    )


def _parse_content_length(headers: Mapping[str, Any]) -> int | None:
    raw = _get_header_ci(headers, "Content-Length")
    if raw is None:
        return None
    try:
        val = int(str(raw).strip())
        return val if val >= 0 else None
    except ValueError:
        return None


def _stream_response_to_part(
    response: Any,
    part_path: Path,
    *,
    offset: int,
    chunk_size: int,
) -> tuple[int, bool, ChecksumResult | None, str | None]:
    status = getattr(response, "status", response.getcode())
    headers = response.headers

    if status == 206:
        content_range = str(_get_header_ci(headers, "Content-Range") or "").strip()
        match = _CONTENT_RANGE_RE.match(content_range)
        if offset > 0:
            if not match:
                part_path.unlink(missing_ok=True)
                raise DownloadError(
                    f"Server returned 206 without valid Content-Range header: {content_range!r}"
                )
            start_byte = int(match.group(1))
            if start_byte == offset:
                mode = "ab"
                resumed = True
            elif start_byte == 0:
                mode = "wb"
                resumed = False
            else:
                part_path.unlink(missing_ok=True)
                raise DownloadError(
                    f"Server returned mismatched Content-Range start={start_byte} for offset={offset}"
                )
        else:
            mode = "wb"
            resumed = False
    elif status == 200:
        # Full response: always truncate any pre-existing .part to avoid corruption.
        mode = "wb"
        resumed = False
    else:
        raise DownloadError(f"Unexpected HTTP status {status} during download.")

    cd_filename = parse_content_disposition_filename(_get_header_ci(headers, "Content-Disposition"))
    expected_len = _parse_content_length(headers)
    written_this_attempt = 0
    sha256_hash = hashlib.sha256() if mode == "wb" else None
    md5_hash = hashlib.md5() if mode == "wb" else None

    with part_path.open(mode) as out_fh:
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            out_fh.write(chunk)
            if sha256_hash is not None and md5_hash is not None:
                sha256_hash.update(chunk)
                md5_hash.update(chunk)
            written_this_attempt += len(chunk)

    if expected_len is not None and written_this_attempt < expected_len:
        raise http.client.IncompleteRead(
            b"",
            expected_len - written_this_attempt,
        )

    stream_checksums = (
        ChecksumResult(
            sha256=sha256_hash.hexdigest(),
            md5=md5_hash.hexdigest(),
            size_bytes=written_this_attempt,
        )
        if sha256_hash is not None and md5_hash is not None
        else None
    )
    return written_this_attempt, resumed, stream_checksums, cd_filename


def download_file_resumable(
    url: str,
    dest_path: str | Path,
    *,
    config: DownloadConfig | None = None,
    expected_sha256: str | None = None,
    expected_md5: str | None = None,
    prefer_content_disposition: bool = False,
    sleep_fn: Callable[[float], None] = time.sleep,
    opener: urllib.request.OpenerDirector | None = None,
) -> DownloadResult:
    """Stream a file to dest_path using .part resume, retries, and exponential backoff."""
    cfg = config or DownloadConfig()
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    part_path = Path(f"{dest}.part")

    if not cfg.resume and part_path.exists():
        part_path.unlink(missing_ok=True)

    open_fn = opener.open if opener is not None else urllib.request.urlopen
    total_downloaded = 0
    used_resume = False
    attempts = 0
    last_error: Exception | None = None
    precomputed_checksums: ChecksumResult | None = None
    cd_filename: str | None = None

    for attempt in range(cfg.retries + 1):
        attempts = attempt + 1
        offset = part_path.stat().st_size if (cfg.resume and part_path.exists()) else 0
        headers: dict[str, str] = {"User-Agent": "ie212-bronze-downloader/0.1"}
        if cfg.resume and offset > 0:
            headers["Range"] = f"bytes={offset}-"

        request = urllib.request.Request(url, headers=headers)
        try:
            with open_fn(request, timeout=cfg.timeout_seconds) as response:
                written, resumed, stream_checksums, cd_filename = _stream_response_to_part(
                    response,
                    part_path,
                    offset=offset,
                    chunk_size=cfg.chunk_size_bytes,
                )
            total_downloaded += written
            used_resume = used_resume or resumed
            precomputed_checksums = stream_checksums
            break
        except urllib.error.HTTPError as exc:
            if exc.code == 416:
                content_range = str(_get_header_ci(exc.headers, "Content-Range") or "").strip()
                match_416 = _CONTENT_RANGE_416_RE.match(content_range)
                if match_416 and offset > 0 and offset == int(match_416.group(1)):
                    # Local .part already holds the exact full remote byte length.
                    used_resume = True
                    precomputed_checksums = None
                    cd_filename = parse_content_disposition_filename(
                        _get_header_ci(exc.headers, "Content-Disposition")
                    )
                    break
                # Offset exceeds remote size or Content-Range invalid: reset .part and retry from 0.
                part_path.unlink(missing_ok=True)
                last_error = exc
            elif exc.code in TRANSIENT_HTTP_STATUS_CODES:
                last_error = exc
            else:
                raise DownloadError(f"HTTP {exc.code} downloading {url}: {exc.reason}") from exc
        except (
            urllib.error.URLError,
            http.client.HTTPException,
            TimeoutError,
            OSError,
            DownloadError,
        ) as exc:
            last_error = exc

        if attempt < cfg.retries:
            delay = cfg.backoff_seconds * (cfg.backoff_multiplier ** attempt)
            if delay > 0:
                sleep_fn(delay)
        else:
            raise DownloadError(
                f"Failed downloading {url} after {attempts} attempts: {last_error}"
            ) from last_error

    try:
        if precomputed_checksums is not None:
            checksums = _verify_checksum_values(
                precomputed_checksums,
                part_path,
                expected_sha256=expected_sha256,
                expected_md5=expected_md5,
            )
        else:
            checksums = verify_file_checksums(
                part_path,
                expected_sha256=expected_sha256,
                expected_md5=expected_md5,
                chunk_size=cfg.chunk_size_bytes,
            )
    except ChecksumMismatchError:
        part_path.unlink(missing_ok=True)
        raise

    final_dest = (dest.parent / cd_filename) if (prefer_content_disposition and cd_filename) else dest
    os.replace(part_path, final_dest)
    return DownloadResult(
        url=url,
        dest_path=final_dest,
        sha256=checksums.sha256,
        md5=checksums.md5,
        size_bytes=checksums.size_bytes,
        bytes_downloaded=total_downloaded,
        resumed=used_resume,
        attempts=attempts,
        content_disposition_filename=cd_filename,
    )


def download_batch(
    tasks: Sequence[DownloadTask],
    *,
    config: DownloadConfig | None = None,
    concurrency: int | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    opener: urllib.request.OpenerDirector | None = None,
) -> list[DownloadResult]:
    """Download multiple files with concurrency strictly bounded to 1..2."""
    cfg = config or DownloadConfig()
    effective_concurrency = cfg.max_concurrency if concurrency is None else concurrency
    validate_concurrency(effective_concurrency, max_allowed=cfg.max_concurrency)

    if not tasks:
        return []

    if effective_concurrency == 1 or len(tasks) == 1:
        return [
            download_file_resumable(
                task.url,
                task.dest_path,
                config=cfg,
                expected_sha256=task.expected_sha256,
                expected_md5=task.expected_md5,
                prefer_content_disposition=task.prefer_content_disposition,
                sleep_fn=sleep_fn,
                opener=opener,
            )
            for task in tasks
        ]

    with ThreadPoolExecutor(max_workers=effective_concurrency) as executor:
        futures = [
            executor.submit(
                download_file_resumable,
                task.url,
                task.dest_path,
                config=cfg,
                expected_sha256=task.expected_sha256,
                expected_md5=task.expected_md5,
                prefer_content_disposition=task.prefer_content_disposition,
                sleep_fn=sleep_fn,
                opener=opener,
            )
            for task in tasks
        ]
        return [future.result() for future in futures]


__all__ = [
    "ChecksumMismatchError",
    "ChecksumResult",
    "DownloadConfig",
    "DownloadError",
    "DownloadResult",
    "DownloadTask",
    "TRANSIENT_HTTP_STATUS_CODES",
    "compute_file_checksums",
    "download_batch",
    "download_file_resumable",
    "parse_content_disposition_filename",
    "validate_concurrency",
    "verify_file_checksums",
]
