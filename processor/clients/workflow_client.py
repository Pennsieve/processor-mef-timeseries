import requests
import json
import logging

from .base_client import BaseClient

log = logging.getLogger()

class WorkflowInstance:
    def __init__(self, id, dataset_id, package_ids):
        self.id = id
        self.dataset_id = dataset_id
        self.package_ids = package_ids

class WorkflowClient(BaseClient):
    def __init__(self, api_host, session_manager):
        super().__init__(session_manager)

        self.api_host = api_host

    # NOTE: workflows API currently returns a 200 response
    #       with an empty body even when a workflow instance does not exist
    @BaseClient.retry_with_refresh
    def get_workflow_instance(self, workflow_instance_id):
        url = f"{self.api_host}/workflows/instances/{workflow_instance_id}"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.session_manager.session_token}"
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            workflow_instance = WorkflowInstance(
                id=data["uuid"],
                dataset_id=data["datasetId"],
                package_ids=data["packageIds"],
            )

            return workflow_instance
        except requests.HTTPError as e:
            log.error(f"failed to fetch workflow instance with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode workflow instance response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to get workflow instance with error: {e}")
            raise e
