from evaluation.evaluate_buyer_matching_mlflow import MockChatModelWithCandidates
from utils import match_string_via_api

def test_mock_deterministic():
    cands = ["Home Office", "Cabinet Office"]
    model = MockChatModelWithCandidates(candidates=cands, similarity_threshold=0.85)
    out1 = model.invoke([type("M", (), {"content":"system"})(), type("M", (), {"content":"Home Office"})()]).content
    out2 = model.invoke([type("M", (), {"content":"system"})(), type("M", (), {"content":"Home Office"})()]).content
    assert out1 == out2

def test_utils_match_string_via_api_returns_string(monkeypatch):
    prompt_path = "prompts/buyer_match_v1.txt"
    cands = ["Home Office", "Cabinet Office"]

    def _fake_http_get(url: str, timeout_s: float = 60.0):
        # ensure we encode candidates as repeated query params
        assert "input_string=Home+Office" in url
        assert "candidates=Cabinet+Office" in url or "candidates=Cabinet%20Office" in url
        assert "prompt_path=prompts%2Fbuyer_match_v1.txt" in url or "prompt_path=prompts/buyer_match_v1.txt" in url
        return 200, '{"input_string":"Home Office","match":"Cabinet Office","raw":"Cabinet Office"}'

    monkeypatch.setenv("MATCH_STRING_API_URL", "http://example.test/match")
    monkeypatch.setattr("utils._http_get", _fake_http_get)

    out = match_string_via_api(input_string="Home Office", list_of_strings=cands, prompt_path=prompt_path)
    assert isinstance(out, str)
    assert out in {"Home Office", "None", "Cabinet Office"}
