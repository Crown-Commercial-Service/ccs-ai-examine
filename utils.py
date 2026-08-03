from __future__ import annotations

import json
import os
import random
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from tenacity import retry, retry_if_exception, stop_after_attempt, wait_random_exponential

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - optional dependency fallback
    tqdm = None

"""Utilities for calling the external matching API."""


class MatchAPIError(RuntimeError):
    """An HTTP or network failure while calling the name-match API."""

    def __init__(
        self,
        message: str,
        *,
        status: Optional[int] = None,
        retryable: bool = False,
        retry_after_s: Optional[float] = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.retryable = retryable
        self.retry_after_s = retry_after_s


def _diagnostic(message: str) -> None:
    """Write a timestamped diagnostic that is visible in DVC's captured output."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [name-match] {message}", file=sys.stderr, flush=True)


def _retry_after_seconds(headers: Any) -> Optional[float]:
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return None


def _http_error(error: urllib.error.HTTPError) -> MatchAPIError:
    try:
        body = (error.read() or b"").decode("utf-8", errors="replace")
    except Exception:
        body = ""

    # Azure App Service sometimes returns 403 with its own HTML "Web App -
    # Unavailable" page when the app is unavailable or access is blocked. A normal
    # application/authentication 403 is permanent and must not be hammered.
    azure_unavailable = error.code == 403 and (
        "Web App - Unavailable" in body or "attempted to reach has blocked" in body
    )
    retryable = error.code in {408, 425, 429, 500, 502, 503, 504} or azure_unavailable
    body_summary = " ".join(body.split())[:1000]
    return MatchAPIError(
        f"Match API error {error.code}: {body_summary}",
        status=error.code,
        retryable=retryable,
        retry_after_s=_retry_after_seconds(error.headers),
    )


def _http_get(url: str, timeout_s: float = 60.0) -> Tuple[int, str]:
    """HTTP GET and return (status_code, response_text)."""
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = int(getattr(resp, "status", 200) or 200)
            text = resp.read().decode("utf-8", errors="replace")
            return status, text
    except urllib.error.HTTPError as error:
        raise _http_error(error) from error
    except (urllib.error.URLError, TimeoutError, socket.timeout) as error:
        raise MatchAPIError(
            f"Match API connection/timeout error after {timeout_s}s: {error!r}",
            retryable=True,
        ) from error


def _http_post_json(
    url: str, payload: Dict[str, Any], timeout_s: float = 60.0
) -> Tuple[int, str]:
    """HTTP POST JSON and return (status_code, response_text)."""
    # Reject NaN/Infinity locally and show the exact field instead of sending invalid JSON.
    try:
        body = json.dumps(payload, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ValueError(f"Invalid match API JSON payload: {error}; payload={payload!r}") from error

    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = int(getattr(resp, "status", 200) or 200)
            text = resp.read().decode("utf-8", errors="replace")
            return status, text
    except urllib.error.HTTPError as error:
        raise _http_error(error) from error
    except (urllib.error.URLError, TimeoutError, socket.timeout) as error:
        raise MatchAPIError(
            f"Match API connection/timeout error after {timeout_s}s: {error!r}",
            retryable=True,
        ) from error


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
    resolved_api_url = api_url or os.getenv("NAME_MATCH_API_ENDPOINT") or os.getenv("MATCH_STRING_API_URL")
    if not resolved_api_url:
        raise ValueError(
            "No API URL provided. Set NAME_MATCH_API_ENDPOINT or pass api_url=... "
            "to match_string_via_api()."
        )

    resolved_api_method = (api_method or os.getenv("MATCH_STRING_API_METHOD", "GET")).strip().upper()
    if resolved_api_method not in {"GET", "POST"}:
        raise ValueError("api_method must be GET or POST")

    if input_string is None or not str(input_string).strip() or str(input_string).strip().lower() == "nan":
        raise ValueError(f"input_string must be a real, nonblank name; got {input_string!r}")

    candidates = [item for item in list_of_strings if item != input_string]
    query: Dict[str, Any] = {"input_string": input_string, "candidates": candidates}
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
        except MatchAPIError as exc:
            if exc.status == 431:
                status, text = _http_post_json(resolved_api_url, query, timeout_s=timeout_s)
            else:
                raise

    if status < 200 or status >= 300:
        raise MatchAPIError(
            f"Match API returned status {status}: {text}",
            status=status,
            retryable=status in {408, 425, 429, 500, 502, 503, 504},
        )

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            if "match" not in data:
                raise KeyError("Missing 'match' in API response JSON.")
            raw_result = "" if data["match"] is None else str(data["match"]).strip()
        elif isinstance(data, str):
            raw_result = data.strip()
        else:
            raise TypeError(f"Unexpected API response JSON type: {type(data).__name__}")
    except (json.JSONDecodeError, TypeError, KeyError):
        raw_result = (text or "").strip()

    if raw_result == "" or raw_result.lower() in {"none", "null", "n/a", "na"}:
        raw_result = "None"
    return raw_result if raw_result in list_of_strings or raw_result == "None" else "None"


def _is_retryable(exception: BaseException) -> bool:
    return isinstance(exception, MatchAPIError) and exception.retryable


def _wait_for_retry(retry_state: Any) -> float:
    exception = retry_state.outcome.exception()
    if isinstance(exception, MatchAPIError) and exception.retry_after_s is not None:
        return exception.retry_after_s + random.uniform(0.0, 1.0)
    # Random exponential backoff prevents all timed-out workers retrying together.
    return float(wait_random_exponential(multiplier=2, min=2, max=30)(retry_state))


def _log_before_retry(retry_state: Any) -> None:
    item = retry_state.kwargs.get("input_string", "<unknown>")
    exception = retry_state.outcome.exception()
    sleep_for = retry_state.next_action.sleep
    status = getattr(exception, "status", None)
    _diagnostic(
        f"RETRY item={item!r} attempt={retry_state.attempt_number}/3 "
        f"next_attempt_in={sleep_for:.1f}s status={status!r} "
        f"error={type(exception).__name__}: {exception}"
    )


@retry(
    stop=stop_after_attempt(3),
    wait=_wait_for_retry,
    retry=retry_if_exception(_is_retryable),
    reraise=True,
    before_sleep=_log_before_retry,
)
def match_string_with_retry(*args: Any, **kwargs: Any) -> str:
    return match_string_via_api(*args, **kwargs)


def _progress_watchdog(
    futures: Dict[Any, str], stop_event: threading.Event, started_at: float, interval_s: float
) -> None:
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
        max_workers: int = 4,
        extra_query_params: Optional[Dict[str, str]] = None,
        api_method: Optional[str] = None,
        show_progress: bool = False,
        progress_desc: str = "Matching names",
        diagnostics: bool = True,
        diagnostic_interval_s: float = 30.0,
        request_delay_s: float = 0.1,  # Added: gentle throttling between submissions
        raise_on_failure: bool = False,  # Added: control whether 1 error kills the batch
) -> Dict[str, str]:
    """Concurrently resolve names with visible diagnostics and bounded concurrency."""
    if max_workers <= 0:
        raise ValueError("max_workers must be > 0")
    if diagnostic_interval_s <= 0:
        raise ValueError("diagnostic_interval_s must be > 0")

    unique_inputs = list(dict.fromkeys(input_strings))
    if not unique_inputs:
        return {}

    if diagnostics:
        _diagnostic(
            f"START requests={len(unique_inputs)} workers={min(max_workers, len(unique_inputs))} "
            f"request_timeout={timeout_s}s attempts=3 method="
            f"{(api_method or os.getenv('MATCH_STRING_API_METHOD', 'GET')).upper()}"
        )

    results: Dict[str, str] = {}
    worker_count = min(max_workers, len(unique_inputs))
    started_at = time.monotonic()
    stop_event = threading.Event()
    watchdog: Optional[threading.Thread] = None

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        # Submit tasks with a slight pause to avoid overwhelming Azure's gateway
        futures = {}
        for item in unique_inputs:
            future = executor.submit(
                match_string_with_retry,
                input_string=item,
                list_of_strings=list_of_strings,
                prompt_path=prompt_path,
                api_url=api_url,
                timeout_s=timeout_s,
                extra_query_params=extra_query_params,
                api_method=api_method,
            )
            futures[future] = item
            if request_delay_s > 0:
                time.sleep(request_delay_s)

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
            completed_futures = tqdm(completed_futures, total=len(futures), desc=progress_desc, unit="name")

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

                    if raise_on_failure:
                        for other_future in futures:
                            if other_future is not future:
                                other_future.cancel()
                        raise RuntimeError(
                            f"Name matching failed for {key!r}; completed "
                            f"{len(results)}/{len(futures)} requests: {exc}"
                        ) from exc
                    else:
                        # Default behavior: record None/empty string for failed item so job completes
                        results[key] = None
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