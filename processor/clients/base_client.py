import requests
import logging

log = logging.getLogger()

# encapsulates a shared API session and re-authentication functionality
class SessionManager:
    def __init__(self, authentication_client, api_key, api_secret):
        self.authentication_client = authentication_client
        self.api_key = api_key
        self.api_secret = api_secret

        self.__session_token = None

    @property
    def session_token(self):
        if self.__session_token is None:
            self.refresh_session()

        return self.__session_token

    def refresh_session(self):
        self.__session_token = self.authentication_client.authenticate(self.api_key, self.api_secret)

class BaseClient:
    def __init__(self, session_manager):
        self.session_manager = session_manager

    def retry_with_refresh(func):
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in (401, 403):
                    log.warning("refreshing session")
                    self.session_manager.refresh_session()
                    return func(self, *args, **kwargs)
                raise
        return wrapper
