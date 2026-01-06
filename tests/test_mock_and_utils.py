from evaluation.evaluate_buyer_matching_mlflow import MockChatModelWithCandidates
from utils import match_string_with_langchain

def test_mock_deterministic():
    cands = ["Home Office", "Cabinet Office"]
    model = MockChatModelWithCandidates(candidates=cands, similarity_threshold=0.85)
    out1 = model.invoke([type("M", (), {"content":"system"})(), type("M", (), {"content":"Home Office"})()]).content
    out2 = model.invoke([type("M", (), {"content":"system"})(), type("M", (), {"content":"Home Office"})()]).content
    assert out1 == out2

def test_utils_match_string_returns_string():
    prompt_path = "prompts/buyer_match_v1.txt"
    cands = ["Home Office", "Cabinet Office"]
    model = MockChatModelWithCandidates(candidates=cands, similarity_threshold=0.85)

    out = match_string_with_langchain(
        input_string="Home Office",
        list_of_strings=cands,
        model=model,
        prompt_path=prompt_path,
    )
    assert isinstance(out, str)
    assert out in {"Home Office", "None", "Cabinet Office"}
