from typing import List
from langchain_core.messages import SystemMessage, HumanMessage
from tqdm import tqdm
from langchain.callbacks.base import BaseCallbackHandler

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

def batch_match_string_with_langchain(input_strings: List[str], list_of_strings: List[str], model) -> List[str]:
    """
    Matches a list of input strings to one of a list of strings using a LangChain model in a batch.

    Args:
        input_strings: The list of strings to match.
        list_of_strings: A list of strings to match against.
        model: The LangChain model object (e.g., a ChatOpenAI instance).

    Returns:
        A list of the model's response contents.
    """
    system_prompt = f"""
    Match the input string to one of these : {list_of_strings}. If you can't find a match, return 'None'.
    """
    
    messages = [
        [
            SystemMessage(content=system_prompt),
            HumanMessage(content=input_string),
        ]
        for input_string in input_strings
    ]
    
    num_requests = len(input_strings)
    tqdm_callback = TqdmCallbackHandler(total=num_requests)
    
    print(f"Using LLM to find matches for {len(input_strings)} strings.")
    responses = model.batch(
        messages, 
        config={"callbacks": [tqdm_callback]}
    )
    print(f"Got {len(responses)} responses.")
    return [response.content for response in responses]


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
    system_prompt = f"""
    Match the input string to one of these : {list_of_strings}. If you can't find a match, return 'None'.
    """
    
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=input_string),
    ]
    print(f"Using LLM to find match for {input_string}")
    response = model.invoke(messages)
    print(f"Response = {response.content}")
    return response.content