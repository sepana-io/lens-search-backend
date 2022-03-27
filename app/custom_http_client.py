import logging

from requests.adapters import HTTPAdapter
from requests import Session
from arango.response import Response
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from arango.http import HTTPClient


class CustomHTTPClient(HTTPClient):

    def __init__(self):
        self._logger = logging.getLogger('my_logger')

    def create_session(self, host):
        session = Session()
        session.headers.update({'x-my-header': 'true'})
        http_adapter = HTTPAdapter()
        session.mount('https://', http_adapter)
        session.mount('http://', http_adapter)

        return session

    def send_request(self,
                     session,
                     method,
                     url,
                     params=None,
                     data=None,
                     headers=None,
                     auth=None):
        self._logger.debug(f'Sending request to {url}')

        response = session.request(
            method=method,
            url=url,
            params=params,
            data=data,
            headers=headers,
            auth=auth,
            verify=False, 
            timeout=10 
        )
        self._logger.debug(f'Got {response.status_code}')

        return Response(
            method=response.request.method,
            url=response.url,
            headers=response.headers,
            status_code=response.status_code,
            status_text=response.reason,
            raw_body=response.text,
        )