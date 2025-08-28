import requests
import json
import logging

from .base_client import BaseClient

log = logging.getLogger()

class ImportFile:
    def __init__(self, upload_key, file_path, local_path):
        self.upload_key=upload_key
        self.file_path=file_path
        self.local_path = local_path
    def __repr__(self):
        return f"ImportFile(upload_key={self.upload_key}, file_path={self.file_path}, local_path={self.local_path})"

class ImportClient(BaseClient):
    def __init__(self, api_host, session_manager):
        super().__init__(session_manager)

        self.api_host = api_host

    @BaseClient.retry_with_refresh
    def create(self, integration_id, dataset_id, package_id, timeseries_files):
        url = f"{self.api_host}/import?dataset_id={dataset_id}"

        headers = {
            "Content-type": "application/json",
            "Authorization": f"Bearer {self.session_manager.session_token}"
        }

        body = {
            "integration_id": integration_id,
            "package_id": package_id,
            "import_type": "timeseries",
            "files": [{"upload_key": str(file.upload_key), "file_path": file.file_path} for file in timeseries_files]
        }

        try:
            response = requests.post(url, headers=headers, json=body)
            response.raise_for_status()
            data = response.json()

            return data['id']
        except requests.HTTPError as e:
            log.error(f"failed to create import with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode import response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to get import with error: {e}")
            raise e

    @BaseClient.retry_with_refresh
    def get_presign_url(self, import_id, dataset_id, upload_key):
        url = f"{self.api_host}/import/{import_id}/upload/{upload_key}/presign?dataset_id={dataset_id}"

        headers = {
            "Content-type": "application/json",
            "Authorization": f"Bearer {self.session_manager.session_token}"
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            return data["url"]
        except requests.HTTPError as e:
            log.error(f"failed to generate pre-sign URL for import file with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode pre-sign URL response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to generate pre-sign URL for import file with error: {e}")
            raise e
