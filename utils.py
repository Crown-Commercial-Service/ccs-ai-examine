from typing import List
from langchain.schema import SystemMessage, HumanMessage

def match_string_with_langchain(input_string: str, list_of_strings: List[str], model) -> str:
    """
    Matches an input string to one of a list of strings using a LangChain model.

    Args:
        input_string: The string to match.
        list_of_strings: A list of strings to match against.
        model: The LangChain model object (e.g., a ChatOpenAI instance).

    Returns:
        The model's response content.
    """
    system_prompt = f"Match the input string to one of these : {', '.join(list_of_strings)}"
    
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=input_string),
    ]

    response = model.invoke(messages)
    return response.content
