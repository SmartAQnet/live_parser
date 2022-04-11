import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def session(con=5, back=3):

    s = requests.Session()
    retry = Retry(connect=con, backoff_factor=back)
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('http://', adapter)
    s.mount('https://', adapter)

    return s
