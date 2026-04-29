import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any, Callable


def ensure_dir(path: str | Path) -> Path:
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def cache_key(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def read_json(path: str | Path) -> Any | None:
    file_path = Path(path)
    if not file_path.exists():
        return None

    try:
        with file_path.open("r", encoding="utf-8") as file:
            return json.load(file)
    except (json.JSONDecodeError, OSError):
        return None


def write_json(path: str | Path, data: Any) -> None:
    file_path = Path(path)
    ensure_dir(file_path.parent)
    with file_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    value = value.casefold().strip()
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_url_for_key(url: str) -> str:
    return normalize_text(url).rstrip("/")


def retry_request(
    request_func: Callable[[], Any],
    retries: int = 2,
    delay_seconds: float = 1.0,
    backoff: float = 2.0,
) -> Any:
    last_error: Exception | None = None

    for attempt in range(retries + 1):
        try:
            return request_func()
        except Exception as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(delay_seconds * (backoff**attempt))

    if last_error:
        raise last_error
    raise RuntimeError("Retry request failed without an exception.")
