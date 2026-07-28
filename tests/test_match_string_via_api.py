import json
from urllib.parse import urlparse, parse_qs

import pytest
import utils


def test_match_string_via_api_returns_exact_candidate(monkeypatch):
    def _fake_http_get(url: str, timeout_s: float = 60.0):
        qs = parse_qs(urlparse(url).query)
        assert qs["input_string"] == ["Home Office"]
        # input_string is removed from candidates before the request
        assert qs["candidates"] == ["Cabinet Office", "HM Treasury"]
        assert qs["prompt_path"] == ["prompts/buyer_match_v1.txt"]

        body = {
            "input_string": "Home Office",
            "match": "Cabinet Office",
            "raw": "Cabinet Office",
        }
        return 200, json.dumps(body)

    monkeypatch.setenv("MATCH_STRING_API_URL", "http://example.test/match")
    monkeypatch.setattr(utils, "_http_get", _fake_http_get)

    out = utils.match_string_via_api(
        input_string="Home Office",
        list_of_strings=["Home Office", "Cabinet Office", "HM Treasury"],
        prompt_path="prompts/buyer_match_v1.txt",
    )
    assert out == "Cabinet Office"


def test_match_string_via_api_null_match_becomes_None(monkeypatch):
    def _fake_http_get(url: str, timeout_s: float = 60.0):
        body = {"input_string": "X", "match": None, "raw": "None"}
        return 200, json.dumps(body)

    monkeypatch.setenv("MATCH_STRING_API_URL", "http://example.test/match")
    monkeypatch.setattr(utils, "_http_get", _fake_http_get)

    out = utils.match_string_via_api(input_string="X", list_of_strings=["A", "B"])
    assert out == "None"


def test_match_string_via_api_rejects_non_candidate(monkeypatch):
    def _fake_http_get(url: str, timeout_s: float = 60.0):
        body = {
            "input_string": "X",
            "match": "Not a candidate",
            "raw": "Not a candidate",
        }
        return 200, json.dumps(body)

    monkeypatch.setenv("MATCH_STRING_API_URL", "http://example.test/match")
    monkeypatch.setattr(utils, "_http_get", _fake_http_get)

    out = utils.match_string_via_api(input_string="X", list_of_strings=["A", "B"])
    assert out == "None"


def test_match_string_via_api_raises_on_500_status(monkeypatch):
    def _fake_http_get(url: str, timeout_s: float = 60.0):
        return 500, "internal error"

    monkeypatch.setenv("MATCH_STRING_API_URL", "http://example.test/match")
    monkeypatch.setattr(utils, "_http_get", _fake_http_get)

    with pytest.raises(RuntimeError, match="Match API returned status 500"):
        utils.match_string_via_api(input_string="X", list_of_strings=["A", "B"])


def test_match_string_via_api_propagates_http_errors(monkeypatch):
    def _fake_http_get(url: str, timeout_s: float = 60.0):
        raise RuntimeError("Match API error 500: boom")

    monkeypatch.setenv("MATCH_STRING_API_URL", "http://example.test/match")
    monkeypatch.setattr(utils, "_http_get", _fake_http_get)

    with pytest.raises(RuntimeError, match="Match API error 500: boom"):
        utils.match_string_via_api(input_string="X", list_of_strings=["A", "B"])


def test_match_string_via_api_retries_with_post_on_431(monkeypatch):
    calls = {"get": 0, "post": 0}

    def _fake_http_get(url: str, timeout_s: float = 60.0):
        calls["get"] += 1
        raise RuntimeError("Match API error 431: Request Header Fields Too Large")

    def _fake_http_post_json(url: str, payload, timeout_s: float = 60.0):
        calls["post"] += 1
        assert payload["input_string"] == "Home Office"
        assert "Cabinet Office" in payload["candidates"]
        body = {
            "input_string": "Home Office",
            "match": "Cabinet Office",
            "raw": "Cabinet Office",
        }
        return 200, json.dumps(body)

    monkeypatch.setenv("MATCH_STRING_API_URL", "http://example.test/match")
    monkeypatch.setattr(utils, "_http_get", _fake_http_get)
    monkeypatch.setattr(utils, "_http_post_json", _fake_http_post_json)

    out = utils.match_string_via_api(
        input_string="Home Office",
        list_of_strings=["Home Office", "Cabinet Office", "HM Treasury"],
    )

    assert calls == {"get": 1, "post": 1}
    assert out == "Cabinet Office"


def test_match_string_via_api_uses_post_when_configured(monkeypatch):
    def _fake_http_post_json(url: str, payload, timeout_s: float = 60.0):
        assert url == "http://example.test/match"
        assert payload["input_string"] == "X"
        assert payload["candidates"] == ["A", "B"]
        return 200, json.dumps({"input_string": "X", "match": "A", "raw": "A"})

    monkeypatch.setenv("MATCH_STRING_API_URL", "http://example.test/match")
    monkeypatch.setenv("MATCH_STRING_API_METHOD", "POST")
    monkeypatch.setattr(utils, "_http_post_json", _fake_http_post_json)

    out = utils.match_string_via_api(input_string="X", list_of_strings=["A", "B"])
    assert out == "A"


def test_match_strings_via_api_concurrent_returns_mapping(monkeypatch):
    def _fake_http_get(url: str, timeout_s: float = 60.0):
        qs = parse_qs(urlparse(url).query)
        name = qs["input_string"][0]
        body = {"input_string": name, "match": "A", "raw": "A"}
        return 200, json.dumps(body)

    monkeypatch.setenv("MATCH_STRING_API_URL", "http://example.test/match")
    monkeypatch.setattr(utils, "_http_get", _fake_http_get)

    out = utils.match_strings_via_api_concurrent(
        input_strings=["x", "y", "z"],
        list_of_strings=["A", "B"],
        max_workers=3,
    )

    assert out == {"x": "A", "y": "A", "z": "A"}


def test_match_strings_via_api_concurrent_rejects_invalid_workers():
    with pytest.raises(ValueError, match="max_workers must be > 0"):
        utils.match_strings_via_api_concurrent(
            input_strings=["x"],
            list_of_strings=["A"],
            max_workers=0,
        )


def test_match_strings_via_api_concurrent_uses_expected_worker_count(monkeypatch):
    observed = {"max_workers": None}

    def _fake_match_string_via_api(
        input_string,
        list_of_strings,
        prompt_path=None,
        api_url=None,
        timeout_s=60.0,
        extra_query_params=None,
        api_method=None,
    ):
        return "A"

    class _FakeFuture:
        def __init__(self, result_value):
            self._result_value = result_value

        def result(self):
            return self._result_value

    class _FakeThreadPoolExecutor:
        def __init__(self, max_workers):
            observed["max_workers"] = max_workers

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, **kwargs):
            return _FakeFuture(fn(**kwargs))

    monkeypatch.setattr(utils, "match_string_via_api", _fake_match_string_via_api)
    monkeypatch.setattr(utils, "ThreadPoolExecutor", _FakeThreadPoolExecutor)
    monkeypatch.setattr(utils, "as_completed", lambda futures: list(futures))

    out = utils.match_strings_via_api_concurrent(
        input_strings=["x", "y", "y"],
        list_of_strings=["A", "B"],
        max_workers=10,
    )

    # unique inputs are ["x", "y"], so worker count is capped at 2.
    assert observed["max_workers"] == 2
    assert out == {"x": "A", "y": "A"}
