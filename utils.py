from __future__ import annotations

"""Utilities for batched calls to the external name-matching API."""

import json
import logging
import os
import random
import re
import socket
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any, Dict, List, MutableSet, Optional, Tuple

from tenacity import retry, retry_if_exception, stop_after_attempt, wait_random_exponential

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    tqdm = None

logger = logging.getLogger("name-match")
_RETRYABLE = {408, 425, 429, 500, 502, 503, 504}


class MatchAPIError(RuntimeError):
    def __init__(self, message: str, *, status: Optional[int] = None,
                 retryable: bool = False, retry_after_s: Optional[float] = None) -> None:
        super().__init__(message)
        self.status = status
        self.retryable = retryable
        self.retry_after_s = retry_after_s


def _diagnostic(message: str) -> None:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] [name-match] {message}", file=sys.stderr, flush=True)


def _retry_after_seconds(headers: Any, body: str = "") -> Optional[float]:
    value = headers.get("Retry-After") if headers is not None else None
    if value:
        try:
            return max(0.0, float(value))
        except (TypeError, ValueError):
            pass
    match = re.search(r"retry after\s+(\d+(?:\.\d+)?)\s*(ms|milliseconds?|s|seconds?)", body, re.I)
    if match:
        amount = float(match.group(1))
        return amount / 1000 if match.group(2).lower().startswith("m") else amount
    return None


def _contains_upstream_429(body: str) -> bool:
    """Detect an Azure 429 incorrectly wrapped by the FastAPI service as HTTP 500."""
    lowered = body.lower()
    return "429" in lowered and any(
        marker in lowered for marker in ("rate_limit", "rate limit", "too_many_requests")
    )


def _http_error(error: urllib.error.HTTPError) -> MatchAPIError:
    try:
        body = (error.read() or b"").decode("utf-8", errors="replace")
    except Exception:
        body = ""
    effective_status = 429 if _contains_upstream_429(body) else error.code
    azure_unavailable = error.code == 403 and (
        "Web App - Unavailable" in body or "attempted to reach has blocked" in body
    )
    summary = " ".join(body.split())[:1000]
    return MatchAPIError(
        f"Match API error {error.code}: {summary}",
        status=effective_status,
        retryable=effective_status in _RETRYABLE or azure_unavailable,
        retry_after_s=_retry_after_seconds(error.headers, body),
    )


def _http_post_json(url: str, payload: Dict[str, Any], timeout_s: float = 60.0) -> Tuple[int, str]:
    try:
        body = json.dumps(payload, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid match API JSON payload: {exc}") from exc
    request = urllib.request.Request(
        url, data=body, method="POST", headers={"Content-Type": "application/json", "Accept": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            return int(getattr(response, "status", 200) or 200), response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raise _http_error(exc) from exc
    except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
        raise MatchAPIError(
            f"Match API connection/timeout error after {timeout_s}s: {exc!r}", retryable=True
        ) from exc


def _resolved_api_url(api_url: Optional[str]) -> str:
    value = api_url or os.getenv("NAME_MATCH_API_ENDPOINT") or os.getenv("MATCH_STRING_API_URL")
    if not value:
        raise ValueError("Set NAME_MATCH_API_ENDPOINT or MATCH_STRING_API_URL, or pass api_url")
    return value


def _valid_input(value: Any) -> bool:
    return value is not None and bool(str(value).strip()) and str(value).strip().lower() != "nan"


def match_batch_via_api(
    input_strings: List[str], list_of_strings: List[str], prompt_path: Optional[str] = None,
    api_url: Optional[str] = None, timeout_s: float = 60.0,
    extra_query_params: Optional[Dict[str, Any]] = None, api_method: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Send one JSON POST batch and return ``input string -> match``."""
    del api_method
    if not input_strings or any(not _valid_input(item) for item in input_strings):
        raise ValueError("input_strings must contain at least one real, nonblank name")
    if not list_of_strings:
        raise ValueError("list_of_strings must contain at least one candidate")
    inputs = [str(item) for item in input_strings]
    candidates = [str(item) for item in list_of_strings]
    payload: Dict[str, Any] = {"input_strings": inputs, "candidates": candidates}
    if prompt_path:
        payload["prompt_path"] = prompt_path
    if extra_query_params:
        payload.update(extra_query_params)
    status, text = _http_post_json(_resolved_api_url(api_url), payload, timeout_s)
    if not 200 <= status < 300:
        effective = 429 if _contains_upstream_429(text) else status
        raise MatchAPIError(
            f"Match API returned status {status}: {text[:1000]}", status=effective,
            retryable=effective in _RETRYABLE, retry_after_s=_retry_after_seconds(None, text),
        )
    try:
        rows = json.loads(text)["results"]
        if not isinstance(rows, list):
            raise TypeError("results is not a list")
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise MatchAPIError(f"Invalid match API response: {text[:1000]!r}") from exc
    requested, candidate_set = set(inputs), set(candidates)
    output: Dict[str, Optional[str]] = {}
    for row in rows:
        if not isinstance(row, dict) or "input_string" not in row or "match" not in row:
            raise MatchAPIError(f"Invalid result in match API response: {text[:1000]!r}")
        item = str(row["input_string"])
        if item not in requested:
            continue
        if item in output:
            raise MatchAPIError(f"Duplicate result for {item!r} in match API response")
        match = row["match"]
        if match is None:
            output[item] = None
        else:
            value = str(match).strip()
            output[item] = value if value in candidate_set else None
    missing = requested.difference(output)
    if missing:
        # A missing row is not a valid LLM no-match. Fail the batch so these names
        # enter the dead-letter queue and remain retryable on a subsequent run.
        raise MatchAPIError(
            f"Match API response omitted {len(missing)} requested item(s): "
            f"{sorted(missing)[:10]!r}",
            retryable=True,
        )
    return output


def match_batch_via_api_single_attempt(batch: List[str], candidates: List[str], **kwargs: Any) -> Dict[str, Optional[str]]:
    return match_batch_via_api(batch, candidates, **kwargs)


def match_string_via_api(input_string: str, list_of_strings: List[str], **kwargs: Any) -> str:
    result = match_batch_via_api([input_string], list_of_strings, **kwargs)[str(input_string)]
    return "None" if result is None else result


def _is_retryable(exception: BaseException) -> bool:
    return isinstance(exception, MatchAPIError) and exception.retryable


def _wait_for_retry(state: Any) -> float:
    exc = state.outcome.exception()
    if isinstance(exc, MatchAPIError) and exc.retry_after_s is not None:
        return exc.retry_after_s + random.uniform(0, 1)
    return float(wait_random_exponential(multiplier=2, min=5, max=60)(state))


def _log_before_retry(state: Any) -> None:
    batch = state.kwargs.get("input_strings", state.args[0] if state.args else [])
    exc = state.outcome.exception()
    _diagnostic(
        f"RETRY batch_size={len(batch) if isinstance(batch, list) else '?'} "
        f"attempt={state.attempt_number}/3 next_attempt_in={state.next_action.sleep:.1f}s "
        f"status={getattr(exc, 'status', None)!r} error={exc}"
    )


@retry(stop=stop_after_attempt(3), wait=_wait_for_retry, retry=retry_if_exception(_is_retryable),
       reraise=True, before_sleep=_log_before_retry)
def match_batch_with_retry(*args: Any, **kwargs: Any) -> Dict[str, Optional[str]]:
    return match_batch_via_api(*args, **kwargs)


@retry(stop=stop_after_attempt(3), wait=_wait_for_retry, retry=retry_if_exception(_is_retryable),
       reraise=True, before_sleep=_log_before_retry)
def match_string_with_retry(*args: Any, **kwargs: Any) -> str:
    return match_string_via_api(*args, **kwargs)


def _run_batch_pass(
    batches: List[List[str]], candidates: List[str], *, pass_number: int, max_workers: int,
    request_delay_s: float, diagnostics: bool, diagnostic_interval_s: float,
    started_at: float, call_kwargs: Dict[str, Any], progress: Any = None,
) -> Tuple[Dict[str, Optional[str]], List[Tuple[List[str], Exception]]]:
    """Run a pass with bounded submission and consume completions immediately."""
    results: Dict[str, Optional[str]] = {}
    failures: List[Tuple[List[str], Exception]] = []
    if not batches:
        return results, failures

    workers = min(max_workers, len(batches))
    next_batch = 0
    next_submit_at = time.monotonic()
    completed_batches = 0
    last_completion = time.monotonic()
    last_heartbeat = time.monotonic()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending: Dict[Any, List[str]] = {}
        while next_batch < len(batches) or pending:
            now = time.monotonic()
            if next_batch < len(batches) and len(pending) < workers and now >= next_submit_at:
                batch = batches[next_batch]
                future = executor.submit(
                    match_batch_via_api_single_attempt,
                    batch=batch,
                    candidates=candidates,
                    **call_kwargs,
                )
                pending[future] = batch
                next_batch += 1
                next_submit_at = now + request_delay_s
                continue

            timeout = max(0.0, next_submit_at - now) if next_batch < len(batches) and len(pending) < workers else None
            if diagnostics:
                heartbeat_due = max(0.0, diagnostic_interval_s - (now - last_heartbeat))
                timeout = heartbeat_due if timeout is None else min(timeout, heartbeat_due)

            done, _ = wait(tuple(pending), timeout=timeout, return_when=FIRST_COMPLETED) if pending else (set(), set())
            if not done:
                now = time.monotonic()
                if diagnostics and now - last_heartbeat >= diagnostic_interval_s:
                    _diagnostic(
                        f"PASS {pass_number} HEARTBEAT completed_batches={completed_batches}/{len(batches)} "
                        f"submitted_batches={next_batch}/{len(batches)} in_flight={len(pending)} "
                        f"elapsed={now-started_at:.1f}s "
                        f"no_new_completion_for={now-last_completion:.1f}s"
                    )
                    last_heartbeat = now
                continue

            for future in done:
                batch = pending.pop(future)
                completed_batches += 1
                last_completion = time.monotonic()
                try:
                    values = future.result()
                    results.update({item: values[item] for item in batch})
                    if pass_number == 2:
                        logger.info("[name-match] Pass 2 batch (%d items) RECOVERED.", len(batch))
                except Exception as exc:
                    failures.append((batch, exc))
                    level = logger.warning if pass_number == 1 else logger.error
                    level(
                        "[name-match] Pass %d batch failed (%s).%s",
                        pass_number,
                        exc,
                        f" Moving {len(batch)} items to Pass 2 queue." if pass_number == 1 else "",
                    )
                finally:
                    if progress is not None and pass_number == 1:
                        progress.update(len(batch))
                        progress.set_postfix(
                            completed_batches=completed_batches,
                            deferred_batches=len(failures),
                            refresh=True,
                        )

    return results, failures


def _print_summary_audit(total_inputs: int, total_batches: int, pass1: int, pass2: int,
                         failures: List[Tuple[List[str], str]]) -> None:
    print("\n" + "=" * 80)
    print("                     BATCH MATCHING SUMMARY AUDIT")
    print("=" * 80)
    print(f" Total Items Processed   : {total_inputs}")
    print(f" Total Batches Processed : {total_batches}")
    print(f" Pass 1 Succeeded        : {pass1}")
    print(f" Pass 2 Recovered        : {pass2}")
    print(f" Permanently Failed      : {len(failures)}")
    if failures:
        print("-" * 80)
        for idx, (items, reason) in enumerate(failures, 1):
            print(f"\n--- Failed Batch #{idx} ({len(items)} items) ---\n Reason: {reason}\n Items:")
            for item in items:
                print(f"   - {item}")
    else:
        print("\n[SUCCESS] All batches processed successfully across Pass 1 & Pass 2!")
    print("=" * 80 + "\n")


def match_strings_via_api_concurrent(
    input_strings: List[str], list_of_strings: List[str], prompt_path: Optional[str] = None,
    api_url: Optional[str] = None, timeout_s: float = 60.0, max_workers: int = 4,
    extra_query_params: Optional[Dict[str, Any]] = None, api_method: Optional[str] = None,
    show_progress: bool = False, progress_desc: str = "Matching names", diagnostics: bool = True,
    diagnostic_interval_s: float = 30.0, request_delay_s: float = 0.5,
    raise_on_failure: bool = False, batch_size: Optional[int] = None,
    cooldown_s: Optional[float] = None, max_inputs_per_minute: Optional[float] = None,
    failed_items: Optional[MutableSet[str]] = None,
) -> Dict[str, Optional[str]]:
    """Process batch POSTs with pacing and a two-pass dead-letter queue.

    If ``failed_items`` is supplied, it is populated only with names whose batch
    failed both passes. A returned ``None`` not in that set is a valid LLM no-match.
    """
    del api_method
    if max_workers <= 0 or diagnostic_interval_s <= 0:
        raise ValueError("max_workers and diagnostic_interval_s must be > 0")
    if batch_size is None:
        batch_size = int(os.getenv("MATCH_STRING_BATCH_SIZE", "10"))
    if cooldown_s is None:
        cooldown_s = float(os.getenv("MATCH_STRING_COOLDOWN_SECONDS", "60"))
    if max_inputs_per_minute is None:
        raw_rate = os.getenv("MATCH_STRING_MAX_INPUTS_PER_MINUTE", "0")
        max_inputs_per_minute = float(raw_rate)
    if batch_size <= 0 or cooldown_s < 0 or max_inputs_per_minute < 0:
        raise ValueError("batch_size must be > 0; cooldown/rate must be >= 0")

    unique = list(dict.fromkeys(str(x) for x in input_strings if _valid_input(x)))
    if not unique:
        return {}
    batches = [unique[i:i + batch_size] for i in range(0, len(unique), batch_size)]
    if max_inputs_per_minute:
        request_delay_s = max(request_delay_s, 60.0 * batch_size / max_inputs_per_minute)
    workers = min(max_workers, len(batches))
    started = time.monotonic()
    if diagnostics:
        _diagnostic(
            f"START total_inputs={len(unique)} batches={len(batches)} batch_size={batch_size} "
            f"workers={workers} request_timeout={timeout_s}s method=POST passes=2 "
            f"batch_interval={request_delay_s:.2f}s input_rate_limit={max_inputs_per_minute or 'off'}/min"
        )
    progress = tqdm(total=len(unique), desc=progress_desc, unit="name", file=sys.stderr) if show_progress and tqdm else None
    kwargs = dict(prompt_path=prompt_path, api_url=api_url, timeout_s=timeout_s,
                  extra_query_params=extra_query_params)
    final: List[Tuple[List[str], str]] = []
    pass1 = 0
    pass2 = 0
    try:
        results, failed1 = _run_batch_pass(
            batches, list_of_strings, pass_number=1, max_workers=max_workers,
            request_delay_s=request_delay_s, diagnostics=diagnostics,
            diagnostic_interval_s=diagnostic_interval_s, started_at=started,
            call_kwargs=kwargs, progress=progress)
        pass1 = len(batches) - len(failed1)
        logger.info("[name-match] PASS 1 COMPLETE: %d/%d succeeded.", pass1, len(batches))

        if failed1:
            queued = [batch for batch, _ in failed1]
            if progress is not None:
                progress.set_postfix(stage="cooldown", deferred_batches=len(queued), refresh=True)
            logger.info("[name-match] COOLDOWN: %d batches; sleeping %.1fs.", len(queued), cooldown_s)
            time.sleep(cooldown_s)
            recovered, failed2 = _run_batch_pass(
                queued, list_of_strings, pass_number=2, max_workers=max_workers,
                request_delay_s=request_delay_s, diagnostics=diagnostics,
                diagnostic_interval_s=diagnostic_interval_s, started_at=started, call_kwargs=kwargs)
            results.update(recovered)
            pass2 = len(queued) - len(failed2)
            for batch, exc in failed2:
                results.update({item: None for item in batch})
                final.append((batch, str(exc)))
    finally:
        if progress:
            progress.close()

    if failed_items is not None:
        failed_items.update(item for batch, _ in final for item in batch)

    ordered = {item: results.get(item) for item in unique}
    _print_summary_audit(len(unique), len(batches), pass1, pass2, final)
    if diagnostics:
        _diagnostic(
            f"COMPLETE completed_inputs={len(ordered)}/{len(unique)} pass1_succeeded={pass1} "
            f"pass2_recovered={pass2} permanently_failed_batches={len(final)} "
            f"elapsed={time.monotonic()-started:.1f}s"
        )
    if final and raise_on_failure:
        raise RuntimeError(f"Name matching permanently failed for {sum(len(x) for x, _ in final)} inputs")
    return ordered
