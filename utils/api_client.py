import json
import requests
import logging
import time
import datetime
import decimal

from encode import DecimalEncoder


class ApiClient:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def send_data(self, data, api_url=None):
        if not data:
            return False
        if not api_url:
            api_url = self.config["API"]["url"]
        headers = {"Content-Type": "application/json", "Authorization": f'Bearer {self.config["API"]["api_token"]}'}
        agent_name = self.config["API"]["agent_name"]
        serializable_data = []

        for record in data:
            serializable_record = {}
            serializable_record["agent_name"] = agent_name

            for key, value in record.items():
                if value is None:
                    serializable_record[key] = None
                elif isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
                    serializable_record[key] = value.isoformat()
                elif isinstance(value, (int, float)):
                    serializable_record[key] = value
                elif isinstance(value, decimal.Decimal):
                    serializable_record[key] = float(value)
                elif isinstance(value, bytes):
                    serializable_record[key] = value.decode("utf-8", errors="replace")
                else:
                    serializable_record[key] = str(value)

            serializable_data.append(serializable_record)

        self.logger.info(f"Sending {len(data)} records to API at {api_url}")

        json_data = {"data": serializable_data}
        json_data_str = json.dumps(json_data, cls=DecimalEncoder)

        retry_count = int(self.config["SYNC"].get("retry_count", "3"))

        for attempt in range(retry_count):
            try:
                response = requests.post(api_url, headers=headers, data=json_data_str)

                if response.status_code == 200:
                    self.logger.info(f"Sync successful. Status code: {response.status_code}")
                    return True
                else:
                    self.logger.error(f"Sync failed. Status code: {response.status_code}, Response: {response.text}")
                    if attempt < retry_count - 1:
                        self.logger.info(f"Retrying in 5 seconds... (Attempt {attempt+1}/{retry_count})")
                        time.sleep(5)
                    else:
                        raise Exception(f"Failed after {retry_count} attempts. Status: {response.status_code}")
            except requests.RequestException as e:
                if attempt < retry_count - 1:
                    self.logger.error(f"Request error: {str(e)}. Retrying in 5 seconds... (Attempt {attempt+1}/{retry_count})")
                    time.sleep(5)
                else:
                    raise Exception(f"Connection failed after {retry_count} attempts: {str(e)}")

        return False
