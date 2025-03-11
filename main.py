import tkinter as tk
import logging
import threading
import time
import sys
import os


from utils.config_manager import ConfigManager
from utils.db_connector import DatabaseConnector
from utils.api_client import ApiClient
from ui.app_ui import SyncAgentUI


class SyncAgent:

    def __init__(self, root):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        self.logger = logging.getLogger(__name__)

        self.config_manager = ConfigManager()
        self.config = self.config_manager.load_config()

        self.db_connector = DatabaseConnector(self.config)
        self.api_client = ApiClient(self.config)

        self.ui = SyncAgentUI(root, self.sync_data, self.full_sync)

        if "API" in self.config and "url" in self.config["API"]:
            self.ui.set_url(self.config["API"]["url"])

        last_sync_time = self.config_manager.get_last_sync_time()
        self.ui.update_last_sync(last_sync_time)

        self.auto_sync_running = False
        self.auto_sync_thread = None

        self.start_auto_sync()

    def sync_data(self):
        threading.Thread(target=self._sync_data_worker, daemon=True).start()

    def _sync_data_worker(self):
        try:
            self.ui.update_status("Syncing...")
            self.ui.set_buttons_state(False)

            last_sync_time = self.config_manager.get_last_sync_time()

            data, last_timestamp = self.db_connector.get_data(last_sync_time)

            if not data:
                self.logger.info("No new data to sync")
                self.ui.update_status("Idle")
                self.ui.set_buttons_state(True)
                return

            # Send data to API
            url = self.ui.get_url()
            success = self.api_client.send_data(data, url)

            if success and last_timestamp:
                # Update last sync time in config
                self.config_manager.save_last_sync_time(last_timestamp)
                self.ui.update_last_sync(last_timestamp)

            self.ui.update_status("Idle")
            self.ui.set_buttons_state(True)

        except Exception as e:
            self.logger.error(f"Sync error: {str(e)}")
            self.ui.update_status("Error")
            self.ui.set_buttons_state(True)
            self.ui.show_error("Sync Error", str(e))

    def full_sync(self):
        threading.Thread(target=self._full_sync_worker, daemon=True).start()

    def _full_sync_worker(self):
        try:
            self.ui.update_status("Full Syncing...")
            self.ui.set_buttons_state(False)

            self.logger.info("Starting full sync operation")

            self.ui.update_last_sync(None)

            batch_size = int(self.config["SYNC"].get("batch_size", "1000"))

            total_processed = 0
            has_more_records = True
            last_timestamp = None

            while has_more_records:
                try:
                    data, current_last_timestamp = self.db_connector.get_data(last_timestamp)

                    if not data or len(data) == 0:
                        has_more_records = False
                        self.logger.info(f"Full sync completed. Total records processed: {total_processed}")
                        break

                    url = self.ui.get_url()
                    success = self.api_client.send_data(data, url)

                    if not success:
                        self.logger.warning(f"Failed to sync batch. Stopping full sync.")
                        break

                    total_processed += len(data)
                    self.logger.info(f"Processed batch: {len(data)} records. Total so far: {total_processed}")

                    if current_last_timestamp:
                        last_timestamp = current_last_timestamp
                        final_timestamp = current_last_timestamp

                    if len(data) < batch_size:
                        has_more_records = False

                except Exception as e:
                    self.logger.error(f"Error in full sync batch: {str(e)}")
                    self.ui.show_error("Full Sync Error", str(e))
                    break

            # Save the last record timestamp
            if last_timestamp:
                self.config_manager.save_last_sync_time(last_timestamp)
                self.ui.update_last_sync(last_timestamp)

            self.ui.update_status("Idle")
            self.ui.set_buttons_state(True)

        except Exception as e:
            self.logger.error(f"Full sync error: {str(e)}")
            self.ui.update_status("Error")
            self.ui.set_buttons_state(True)
            self.ui.show_error("Full Sync Error", str(e))

    def start_auto_sync(self):
        if not self.config:
            return

        try:
            interval_minutes = int(self.config["SYNC"].get("interval_minutes", "1"))
            self.auto_sync_running = True
            self.auto_sync_thread = threading.Thread(target=self._auto_sync_worker, args=(interval_minutes,), daemon=True)
            self.auto_sync_thread.start()
            self.logger.info(f"Auto sync started with {interval_minutes} minute interval")
        except Exception as e:
            self.logger.error(f"Failed to start auto sync: {str(e)}")

    def _auto_sync_worker(self, interval_minutes):
        while self.auto_sync_running:
            try:
                self._sync_data_worker()
            except Exception as e:
                self.logger.error(f"Auto sync error: {str(e)}")

            time_elapsed = 0
            while time_elapsed < interval_minutes * 60 and self.auto_sync_running:
                time.sleep(1)
                time_elapsed += 1

    def on_closing(self):
        self.auto_sync_running = False
        self.logger.info("Application shutting down")


if __name__ == "__main__":
    os.makedirs("utils", exist_ok=True)
    os.makedirs("ui", exist_ok=True)

    root = tk.Tk()

    app = SyncAgent(root)
    # root.protocol("WM_DELETE_WINDOW", app.on_closing)

    root.mainloop()
