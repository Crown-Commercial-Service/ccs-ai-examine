from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import List

@dataclass
class MockResponse:
    content: str

@dataclass
class MockChatModelWithCandidates:
    candidates: List[str]
    similarity_threshold: float = 0.85

    def invoke(self, messages):
        input_name = (messages[-1].content or "").strip()
        if not self.candidates:
            return MockResponse("None")

        best = None
        best_score = 0.0
        for c in self.candidates:
            score = SequenceMatcher(None, input_name.lower(), str(c).lower()).ratio()
            if score > best_score:
                best_score = score
                best = c

        if best is None or best_score < self.similarity_threshold:
            return MockResponse("None")
        return MockResponse(str(best))
