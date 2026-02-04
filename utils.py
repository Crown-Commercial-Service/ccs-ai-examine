from __future__ import annotations

import json
from pathlib import Path
from typing import List, Any

from langchain_core.messages import SystemMessage, HumanMessage
from tqdm import tqdm
from langchain_core.callbacks.base import BaseCallbackHandler
import time
import tiktoken

class TqdmCallbackHandler(BaseCallbackHandler):
    """Callback handler for tqdm progress bar."""
    def __init__(self, total: int):
        self.pbar = tqdm(total=total, desc="Batch matching strings")

    def on_llm_end(self, response, **kwargs):
        """Update progress bar on LLM end."""
        self.pbar.update(1)

    def on_llm_error(self, error: Exception, **kwargs):
        """Handle error if needed, maybe close the progress bar."""
        self.pbar.close()

def load_prompt(prompt_path: str) -> str:
    """
    Loads a prompt template from a file.
    """
    p = Path(prompt_path)
    if not p.exists():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")
    return p.read_text(encoding="utf-8")


def match_string_with_langchain(
    input_string: str,
    list_of_strings: List[str],
    model: Any,
    prompt_path: str,
    ) -> str:
    """
    Match an input string to one of a list of candidate strings using a LangChain-style model.

    The prompt file may contain placeholders:
      - {input_name}
      - {candidates}

    Expected model behavior:
      model.invoke(messages) returns an object with `.content` string

    Expected output:
      - EXACT candidate string (must match one element in list_of_strings) OR
      - "None"
    """
    # remove input string from candidates if present
    candidates = [i for i in list_of_strings if i != input_string]
    prompt_template = load_prompt(prompt_path)
    candidates_json = json.dumps(candidates, ensure_ascii=False)

    # replace only the placeholders we own
    system_prompt = (
        prompt_template
        .replace("{input_name}", str(input_string))
        .replace("{candidates}", candidates_json)
    )

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=str(input_string)),
    ]
    print(f"Matching input string {input_string} to {len(list_of_strings)} candidates")
    response = model.invoke(messages)
    raw_result = (getattr(response, "content", "") or "").strip()

    # Only accept the result if it is an EXACT match in our candidate list
    if raw_result in list_of_strings or raw_result == "None":
        print(f"Result = {raw_result}")
        return raw_result
    else:
        # If the LLM returns "None", a hallucination, or a typo, we treat it as no match
        # You can optionally print a warning here to see what it hallucinated
        print(f"Rejected LLM Output: '{raw_result}' (not in candidate list)")
        return "None"
