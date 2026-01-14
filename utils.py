from typing import List
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

def batch_match_string_with_langchain(input_strings: List[str], list_of_strings: List[str], model, tpm_limit: int = 60000, chunk_size: int = 10) -> List[str]:
    """
    Matches a list of input strings to one of a list of strings using a LangChain model in a batch,
    with rate limiting to respect TPM limits.

    Args:
        input_strings: The list of strings to match.
        list_of_strings: A list of strings to match against.
        model: The LangChain model object (e.g., a ChatOpenAI instance).
        tpm_limit: The tokens-per-minute limit of the model.
        chunk_size: The number of requests to send in each chunk.

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

    results = []
    tokens_sent_in_window = 0
    window_start_time = time.time()
    encoding = tiktoken.get_encoding("cl100k_base")

    for i in range(0, len(messages), chunk_size):
        chunk = messages[i:i+chunk_size]
        
        # Estimate token count for the chunk
        token_count = 0
        for message_list in chunk:
            for message in message_list:
                token_count += len(encoding.encode(message.content))

        # Check if this chunk would exceed the TPM limit
        if tokens_sent_in_window + token_count > tpm_limit:
            time_to_wait = 60 - (time.time() - window_start_time)
            if time_to_wait > 0:
                print(f"TPM limit likely to be exceeded. Waiting for {time_to_wait:.2f} seconds.")
                time.sleep(time_to_wait)
            
            # Reset window
            window_start_time = time.time()
            tokens_sent_in_window = 0
        
        num_requests = len(chunk)
        tqdm_callback = TqdmCallbackHandler(total=num_requests)
        
        responses = model.batch(
            chunk, 
            config={"callbacks": [tqdm_callback]}
        )
        results.extend([r.content for r in responses])
        
        tokens_sent_in_window += token_count

    print(f"Got {len(results)} responses.")
    return results


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