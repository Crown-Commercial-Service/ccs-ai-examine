from __future__ import annotations

import os
import json
from typing import List, Any, Optional, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import urllib.parse
import urllib.request
import urllib.error

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - optional dependency fallback
    tqdm = None

"""Utilities for calling the external matching API."""


def _http_get(url: str, timeout_s: float = 60.0) -> Tuple[int, str]:
    """
    Internal helper: HTTP GET and return (status_code, response_text).
    Split out to make it easy to mock in unit tests.
    """
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
    except urllib.error.URLError as e:
        raise RuntimeError(f"Match API connection error: {e}") from e


def _http_post_json(
    url: str, payload: Dict[str, Any], timeout_s: float = 60.0
) -> Tuple[int, str]:
    """
    Internal helper: HTTP POST JSON and return (status_code, response_text).
    """
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
    except urllib.error.URLError as e:
        raise RuntimeError(f"Match API connection error: {e}") from e


def match_string_via_api(
    input_string: str,
    list_of_strings: List[str],
    prompt_path: Optional[str] = None,
    api_url: Optional[str] = None,
    timeout_s: float = 60.0,
    extra_query_params: Optional[Dict[str, str]] = None,
    api_method: Optional[str] = None,
) -> str:
    """
    Call the external matching API (GET /match) instead of running LangChain locally.

    Expected endpoint signature (FastAPI):
      - GET /match?input_string=...&candidates=...&candidates=...&prompt_path=...
      - OR POST /match with JSON body
        {"input_string": "...", "candidates": [...], "prompt_path": "..."}

    Expected response body:
      { "input_string": "...", "match": "<candidate>|null", "raw": "..." }

    Return contract:
      - EXACT candidate string (must match one element in list_of_strings) OR
      - "None"

    Configuration:
      - api_url parameter OR env var NAME_MATCH_API_ENDPOINT must be set to the full URL
        of the `/match` endpoint.
      - api_method parameter OR env var MATCH_STRING_API_METHOD can be set to GET/POST.
        Defaults to GET; when GET fails with HTTP 431 it automatically retries with POST.
    """
    resolved_api_url = api_url or os.getenv("NAME_MATCH_API_ENDPOINT")
    if not resolved_api_url:
        raise ValueError(
            "No API URL provided. Set NAME_MATCH_API_ENDPOINT or pass api_url=... to match_string_via_api()."
        )

    resolved_api_method = (
        (api_method or os.getenv("MATCH_STRING_API_METHOD", "GET")).strip().upper()
    )
    if resolved_api_method not in {"GET", "POST"}:
        raise ValueError("api_method must be GET or POST")

    # Remove input string from candidates if present
    candidates = [i for i in list_of_strings if i != input_string]

    query: Dict[str, Any] = {
        "input_string": input_string,
        "candidates": candidates,  # repeated param via doseq=True
    }
    if prompt_path:
        query["prompt_path"] = prompt_path
    if extra_query_params:
        query.update(extra_query_params)

    status: int
    text: str
    if resolved_api_method == "POST":
        status, text = _http_post_json(resolved_api_url, query, timeout_s=timeout_s)
    else:
        qs = urllib.parse.urlencode(query, doseq=True)
        url = resolved_api_url + ("&" if "?" in resolved_api_url else "?") + qs
        try:
            status, text = _http_get(url, timeout_s=timeout_s)
        except RuntimeError as e:
            if "Match API error 431:" in str(e):
                status, text = _http_post_json(
                    resolved_api_url, query, timeout_s=timeout_s
                )
            else:
                raise

    if status < 200 or status >= 300:
        raise RuntimeError(f"Match API returned status {status}: {text}")

    raw_result = ""

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            if "match" not in data:
                raise KeyError("Missing 'match' in API response JSON.")
            raw_result = "" if data["match"] is None else str(data["match"]).strip()
        elif isinstance(data, str):
            # If the API returns a bare JSON string, treat it as the match.
            raw_result = data.strip()
        else:
            raise TypeError(f"Unexpected API response JSON type: {type(data).__name__}")
    except Exception:
        # If server returns plain text, accept it.
        raw_result = (text or "").strip()

    if raw_result == "" or raw_result.lower() in {"none", "null", "n/a", "na"}:
        raw_result = "None"

    # Validate output (same safety rule as local)
    if raw_result in list_of_strings or raw_result == "None":
        return raw_result
    return "None"


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
) -> Dict[str, str]:
    """
    Concurrently resolve many input strings via match_string_via_api.

    Returns a mapping of input_string -> matched result.
    """
    if max_workers <= 0:
        raise ValueError("max_workers must be > 0")

    unique_inputs: List[str] = []
    for item in input_strings:
        if item not in unique_inputs:
            unique_inputs.append(item)

    if not unique_inputs:
        return {}

    if max_workers == 1:
        serial_results: Dict[str, str] = {}
        items_iter = unique_inputs
        if show_progress and tqdm is not None:
            items_iter = tqdm(
                unique_inputs,
                total=len(unique_inputs),
                desc=progress_desc,
                unit="name",
            )
        for item in items_iter:
            serial_results[item] = match_string_via_api(
                input_string=item,
                list_of_strings=list_of_strings,
                prompt_path=prompt_path,
                api_url=api_url,
                timeout_s=timeout_s,
                extra_query_params=extra_query_params,
                api_method=api_method,
            )
        return serial_results

    results: Dict[str, str] = {}
    worker_count = min(max_workers, len(unique_inputs))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                match_string_via_api,
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
        completed_futures = as_completed(futures)
        if show_progress and tqdm is not None:
            completed_futures = tqdm(
                completed_futures,
                total=len(futures),
                desc=progress_desc,
                unit="name",
            )

        for future in completed_futures:
            key = futures[future]
            results[key] = future.result()

    return results
