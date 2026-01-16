from __future__ import annotations

import json
from pathlib import Path
from typing import List, Any

from langchain_core.messages import SystemMessage, HumanMessage


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
    prompt_template = load_prompt(prompt_path)
    candidates_json = json.dumps(list_of_strings, ensure_ascii=False)

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

    response = model.invoke(messages)
    return (getattr(response, "content", "") or "").strip()
