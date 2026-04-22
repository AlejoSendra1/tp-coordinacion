import logging

RETRY_COUNT = 3

def function_retry(func: function, args: list=[], retry_count: int = None) -> None:
    if retry_count is not None:
        attempts = retry_count
    else:
        attempts = RETRY_COUNT  
    
    while attempts > 0:
        try:
            func(*args)
            break
        except Exception as e:
            logging.error(f"Error: {e}")
        attempts -= 1
    

