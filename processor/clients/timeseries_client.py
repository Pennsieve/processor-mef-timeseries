import requests
import json
import logging

from .base_client import BaseClient
from processor.timeseries_channel import TimeSeriesChannel

log = logging.getLogger()

class TimeSeriesClient(BaseClient):
    def __init__(self, api_host, session_manager):
        super().__init__(session_manager)

        self.api_host = api_host

    @BaseClient.retry_with_refresh
    def create_channel(self, package_id, channel):
        url = f"{self.api_host}/timeseries/{package_id}/channels"

        headers = {
            "Content-type": "application/json",
            "Authorization": f"Bearer {self.session_manager.session_token}"
        }

        body = channel.as_dict()
        body['channelType'] = body.pop('type')

        try:
            response = requests.post(url, headers=headers, json=body)
            response.raise_for_status()
            data = response.json()
            created_channel = TimeSeriesChannel.from_dict(data['content'], data['properties'])
            created_channel.index = channel.index

            return created_channel
        except requests.HTTPError as e:
            log.error("failed to create time series channel: %s", e)
            raise e
        except json.JSONDecodeError as e:
            log.error("failed to decode time series channel response: %s", e)
            raise e
        except Exception as e:
            log.error("failed to create time series channel: %s", e)
            raise e

    @BaseClient.retry_with_refresh
    def get_package_channels(self, package_id):
        url = f"{self.api_host}/timeseries/{package_id}/channels"

        headers = {
            "Content-type": "application/json",
            "Authorization": f"Bearer {self.session_manager.session_token}"
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            channels = []
            for item in data:
                content = item["content"]
                properties = item["properties"]

                channel = TimeSeriesChannel.from_dict(content, properties)
                channels.append(channel)

            return channels
        except requests.HTTPError as e:
            log.error("failed to fetch time series channels for package %s: %s", package_id, e)
            raise e
        except json.JSONDecodeError as e:
            log.error("failed to decode time series package channels response: %s", e)
            raise e
        except Exception as e:
            log.error("failed to time series channels: %s", e)
            raise e
