from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1,min=2,max=30),
    retry=retry_if_exception_type((requests.exceprions.Timeout,requests.exceprions.ConnectionError))
)
def chamar_com_retry(fn):
    return fn()