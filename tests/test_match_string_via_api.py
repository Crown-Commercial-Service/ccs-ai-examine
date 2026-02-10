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

        body = {"input_string": "Home Office", "match": "Cabinet Office", "raw": "Cabinet Office"}
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
        body = {"input_string": "X", "match": "Not a candidate", "raw": "Not a candidate"}
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

