from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from tenacity import retry, stop_after_attempt, wait_exponential

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - optional dependency fallback
    tqdm = None

"""Utilities for calling the external matching API."""


def _diagnostic(message: str) -> None:
    """Write a timestamped diagnostic that is visible in DVC's captured output."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [name-match] {message}", file=sys.stderr, flush=True)


def _http_get(url: str, timeout_s: float = 60.0) -> Tuple[int, str]:
    """HTTP GET and return (status_code, response_text)."""
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = int(getattr(resp, "status", 200) or 200)
            text = resp.read().decode("utf-8", errors="replace")
            return status, text
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = (e.read() or b"").decode("utf-8", errors="replace")
        except Exception:
            body = ""
        raise RuntimeError(f"Match API error {e.code}: {body}") from e
    except (urllib.error.URLError, TimeoutError, socket.timeout) as e:
        raise RuntimeError(
            f"Match API connection/timeout error after {timeout_s}s: {e!r}"
        ) from e


def _http_post_json(
    url: str, payload: Dict[str, Any], timeout_s: float = 60.0
) -> Tuple[int, str]:
    """HTTP POST JSON and return (status_code, response_text)."""
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = int(getattr(resp, "status", 200) or 200)
            text = resp.read().decode("utf-8", errors="replace")
            return status, text
    except urllib.error.HTTPError as e:
        body_text = ""
        try:
            body_text = (e.read() or b"").decode("utf-8", errors="replace")
        except Exception:
            body_text = ""
        raise RuntimeError(f"Match API error {e.code}: {body_text}") from e
    except (urllib.error.URLError, TimeoutError, socket.timeout) as e:
        raise RuntimeError(
            f"Match API connection/timeout error after {timeout_s}s: {e!r}"
        ) from e


def match_string_via_api(
    input_string: str,
    list_of_strings: List[str],
    prompt_path: Optional[str] = None,
    api_url: Optional[str] = None,
    timeout_s: float = 60.0,
    extra_query_params: Optional[Dict[str, str]] = None,
    api_method: Optional[str] = None,
) -> str:
    """Call the external matching API and return an exact candidate or ``None``."""
    # NAME_MATCH_API_ENDPOINT is the current name. Keep the old name as a fallback
    # because existing environments and tests may still use it.
    resolved_api_url = (
        api_url
        or os.getenv("NAME_MATCH_API_ENDPOINT")
        or os.getenv("MATCH_STRING_API_URL")
    )
    if not resolved_api_url:
        raise ValueError(
            "No API URL provided. Set NAME_MATCH_API_ENDPOINT or pass api_url=... "
            "to match_string_via_api()."
        )

    resolved_api_method = (
        (api_method or os.getenv("MATCH_STRING_API_METHOD", "GET")).strip().upper()
    )
    if resolved_api_method not in {"GET", "POST"}:
        raise ValueError("api_method must be GET or POST")

    candidates = [item for item in list_of_strings if item != input_string]
    query: Dict[str, Any] = {
        "input_string": input_string,
        "candidates": candidates,
    }
    if prompt_path:
        query["prompt_path"] = prompt_path
    if extra_query_params:
        query.update(extra_query_params)

    if resolved_api_method == "POST":
        status, text = _http_post_json(resolved_api_url, query, timeout_s=timeout_s)
    else:
        qs = urllib.parse.urlencode(query, doseq=True)
        url = resolved_api_url + ("&" if "?" in resolved_api_url else "?") + qs
        try:
            status, text = _http_get(url, timeout_s=timeout_s)
        except RuntimeError as exc:
            if "Match API error 431:" in str(exc):
                status, text = _http_post_json(
                    resolved_api_url, query, timeout_s=timeout_s
                )
            else:
                raise

    if status < 200 or status >= 300:
        raise RuntimeError(f"Match API returned status {status}: {text}")

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            if "match" not in data:
                raise KeyError("Missing 'match' in API response JSON.")
            raw_result = "" if data["match"] is None else str(data["match"]).strip()
        elif isinstance(data, str):
            raw_result = data.strip()
        else:
            raise TypeError(
                f"Unexpected API response JSON type: {type(data).__name__}"
            )
    except (json.JSONDecodeError, TypeError, KeyError):
        # Preserve support for an API which deliberately returns plain text.
        raw_result = (text or "").strip()

    if raw_result == "" or raw_result.lower() in {"none", "null", "n/a", "na"}:
        raw_result = "None"

    if raw_result in list_of_strings or raw_result == "None":
        return raw_result
    return "None"


def _log_before_retry(retry_state: Any) -> None:
    """Tenacity callback: expose each retry and its underlying exception."""
    item = retry_state.kwargs.get("input_string", "<unknown>")
    exception = retry_state.outcome.exception()
    sleep_for = retry_state.next_action.sleep
    _diagnostic(
        f"RETRY item={item!r} attempt={retry_state.attempt_number}/3 "
        f"next_attempt_in={sleep_for:.1f}s error={type(exception).__name__}: {exception}"
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
    before_sleep=_log_before_retry,
)
def match_string_with_retry(*args: Any, **kwargs: Any) -> str:
    return match_string_via_api(*args, **kwargs)


def _progress_watchdog(
    futures: Dict[Any, str],
    stop_event: threading.Event,
    started_at: float,
    interval_s: float,
) -> None:
    """Report pending work periodically so a slow API cannot look like a frozen DVC run."""
    previous_done = 0
    stagnant_for = 0.0
    while not stop_event.wait(interval_s):
        done = sum(future.done() for future in futures)
        pending = len(futures) - done
        stagnant_for = stagnant_for + interval_s if done == previous_done else 0.0
        previous_done = done
        _diagnostic(
            f"HEARTBEAT completed={done}/{len(futures)} pending={pending} "
            f"elapsed={time.monotonic() - started_at:.1f}s "
            f"no_new_completion_for={stagnant_for:.1f}s. "
            "If this repeats, requests are waiting on the API/network; they are not CPU-bound."
        )


def match_strings_via_api_concurrent(
    input_strings: List[str],
    list_of_strings: List[str],
    prompt_path: Optional[str] = None,
    api_url: Optional[str] = None,
    timeout_s: float = 60.0,
    max_workers: int = 8,
    extra_query_params: Optional[Dict[str, str]] = None,
    api_method: Optional[str] = None,
    show_progress: bool = False,
    progress_desc: str = "Matching names",
    diagnostics: bool = True,
    diagnostic_interval_s: float = 30.0,
) -> Dict[str, str]:
    """
    Concurrently resolve input strings via the API.

    Diagnostics are written to stderr (and therefore shown by DVC): retries include
    the item and underlying error, a heartbeat distinguishes waiting from a frozen
    process, and a final failure reports the completed/pending counts.
    """
    if max_workers <= 0:
        raise ValueError("max_workers must be > 0")
    if diagnostic_interval_s <= 0:
        raise ValueError("diagnostic_interval_s must be > 0")

    # dict preserves order and avoids the O(n^2) membership checks used previously.
    unique_inputs = list(dict.fromkeys(input_strings))
    if not unique_inputs:
        return {}

    if diagnostics:
        _diagnostic(
            f"START requests={len(unique_inputs)} workers={min(max_workers, len(unique_inputs))} "
            f"request_timeout={timeout_s}s attempts=3 method="
            f"{(api_method or os.getenv('MATCH_STRING_API_METHOD', 'GET')).upper()}"
        )

    if max_workers == 1:
        serial_results: Dict[str, str] = {}
        items_iter: Any = unique_inputs
        if show_progress and tqdm is not None:
            items_iter = tqdm(
                unique_inputs,
                total=len(unique_inputs),
                desc=progress_desc,
                unit="name",
            )
        for index, item in enumerate(items_iter, start=1):
            try:
                serial_results[item] = match_string_with_retry(
                    input_string=item,
                    list_of_strings=list_of_strings,
                    prompt_path=prompt_path,
                    api_url=api_url,
                    timeout_s=timeout_s,
                    extra_query_params=extra_query_params,
                    api_method=api_method,
                )
            except Exception as exc:
                _diagnostic(
                    f"FAILED item={item!r} completed={index - 1}/{len(unique_inputs)} "
                    f"error={type(exc).__name__}: {exc}"
                )
                raise RuntimeError(
                    f"Name matching failed for {item!r} after 3 attempts: {exc}"
                ) from exc
        return serial_results

    results: Dict[str, str] = {}
    worker_count = min(max_workers, len(unique_inputs))
    started_at = time.monotonic()
    stop_event = threading.Event()
    watchdog: Optional[threading.Thread] = None

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                match_string_with_retry,
                input_string=item,
                list_of_strings=list_of_strings,
                prompt_path=prompt_path,
                api_url=api_url,
                timeout_s=timeout_s,
                extra_query_params=extra_query_params,
                api_method=api_method,
            ): item
            for item in unique_inputs
        }

        # Fake Future implementations in unit tests need not provide done().
        supports_watchdog = futures and hasattr(next(iter(futures)), "done")
        if diagnostics and supports_watchdog:
            watchdog = threading.Thread(
                target=_progress_watchdog,
                args=(futures, stop_event, started_at, diagnostic_interval_s),
                daemon=True,
                name="name-match-watchdog",
            )
            watchdog.start()

        completed_futures: Any = as_completed(futures)
        if show_progress and tqdm is not None:
            completed_futures = tqdm(
                completed_futures,
                total=len(futures),
                desc=progress_desc,
                unit="name",
            )

        try:
            for future in completed_futures:
                key = futures[future]
                try:
                    results[key] = future.result()
                except Exception as exc:
                    pending = len(futures) - len(results) - 1
                    _diagnostic(
                        f"FAILED item={key!r} completed={len(results)}/{len(futures)} "
                        f"pending={pending} elapsed={time.monotonic() - started_at:.1f}s "
                        f"error={type(exc).__name__}: {exc}"
                    )
                    for other_future in futures:
                        if other_future is not future:
                            other_future.cancel()
                    raise RuntimeError(
                        f"Name matching failed for {key!r} after 3 attempts; "
                        f"completed {len(results)}/{len(futures)} requests: {exc}"
                    ) from exc
        finally:
            stop_event.set()
            if watchdog is not None:
                watchdog.join(timeout=1.0)

    if diagnostics:
        _diagnostic(
            f"COMPLETE completed={len(results)}/{len(futures)} "
            f"elapsed={time.monotonic() - started_at:.1f}s"
        )
    return results
