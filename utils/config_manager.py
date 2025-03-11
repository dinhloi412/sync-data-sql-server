import configparser
import os
import logging
import datetime


class ConfigManager:

    def __init__(self):
        self.config = None
        self.logger = logging.getLogger(__name__)

    def load_config(self):
        try:
            if not os.path.exists("config.ini"):
                self.create_default_config()

            self.config = configparser.ConfigParser()
            self.config.read("config.ini")

            self.logger.info("Config loaded successfully")
            return self.config
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            raise

    def create_default_config(self):
        config = configparser.ConfigParser()

        config["DATABASE"] = {
            "server": "localhost",
            "database": "your_database",
            "table": "your_table",
            "username": "your_username",
            "password": "your_password",
            "driver": "ODBC Driver 17 for SQL Server",
        }

        config["API"] = {"url": "https://api.example.com/sync", "agent_name": "sync_agent_1", "api_token": "your_api_token"}

        config["SYNC"] = {"interval_minutes": "1", "last_sync": "Never", "batch_size": "1000", "retry_count": "3"}

        with open("config.ini", "w") as f:
            config.write(f)

        self.logger.info("Created default config file. Please update with your settings.")

    def save_last_sync_time(self, timestamp=None):
        """Save the timestamp of the last synchronized record"""
        if not self.config:
            return

        if timestamp is None:
            timestamp = datetime.datetime.now()

        if "SYNC" not in self.config:
            self.config["SYNC"] = {}

        self.config["SYNC"]["last_sync"] = timestamp.isoformat(timespec="seconds")

        with open("config.ini", "w") as f:
            self.config.write(f)

        self.logger.info(f"Updated last sync time to {timestamp.isoformat(timespec='seconds')}")

    def get_last_sync_time(self):
        if not self.config or "SYNC" not in self.config or "last_sync" not in self.config["SYNC"]:
            return None

        last_sync_str = self.config["SYNC"]["last_sync"]
        if last_sync_str == "Never":
            return None

        return datetime.datetime.fromisoformat(last_sync_str)
