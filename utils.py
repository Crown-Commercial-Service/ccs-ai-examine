from __future__ import annotations

import json
from pathlib import Path
from typing import List, Any

from langchain_core.messages import SystemMessage, HumanMessage
from openai import BadRequestError
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
    Handles BadRequestError by returning "None".
    """
    # remove input string from candidates if present
    candidates = [i for i in list_of_strings if i != input_string]
    prompt_template = load_prompt(prompt_path)
    candidates_json = json.dumps(candidates, ensure_ascii=False)

    # Replace placeholders
    system_prompt = (
        prompt_template
        .replace("{input_name}", str(input_string))
        .replace("{candidates}", candidates_json)
    )

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=str(input_string)),
    ]

    print(f"Matching name {input_string}")
    try:
        # Attempt to invoke the model
        response = model.invoke(messages)
        content = getattr(response, "content", "").strip()
        # Only accept the result if it is an EXACT match in our candidate list
        if content in list_of_strings or content == "None":
            print(f"Result = {content}")
            return content
        else:
            # If the LLM returns "None", a hallucination, or a typo, we treat it as no match
            # You can optionally print a warning here to see what it hallucinated
            print(f"Rejected LLM Output: '{content}' (not in candidate list)")
            return "None"

    except BadRequestError as e:
        # Catch the specific error, log it, and return a safe fallback
        print(f"Skipping '{input_string}' due to BadRequestError: {e}")
        return "None"

    except Exception as e:
        # Optional: Catch other unexpected errors (e.g., RateLimitError)
        print(f"An unexpected error occurred for '{input_string}': {e}")
        return "None"
