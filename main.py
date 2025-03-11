import decimal
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import configparser
import os
import json
from encode import DecimalEncoder
import pyodbc
import requests
import datetime
import threading
import time
import logging

from text_handler import TextHandler


class SyncAgent:
    def __init__(self, root):
        self.root = root
        self.root.title("SQL Server Sync Agent")
        self.root.geometry("800x600")

        # Configuration
        self.config = None
        self.last_sync_time = None
        self.auto_sync_thread = None
        self.auto_sync_running = False

        # Set up logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        self.logger = logging.getLogger(__name__)

        # Create UI elements
        self.create_ui()

        # Load config
        self.load_config()

        # Start auto sync if enabled
        self.start_auto_sync()

    def create_ui(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # URL input
        url_frame = ttk.Frame(main_frame)
        url_frame.pack(fill=tk.X, pady=5)

        ttk.Label(url_frame, text="API URL:").pack(side=tk.LEFT, padx=(0, 5))
        self.url_entry = ttk.Entry(url_frame, width=50)
        self.url_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Buttons frame
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=5)

        self.sync_btn = ttk.Button(button_frame, text="Sync Data", command=self.sync_data)
        self.sync_btn.pack(side=tk.LEFT, padx=(0, 5))

        self.full_sync_btn = ttk.Button(button_frame, text="Full Sync", command=self.full_sync)
        self.full_sync_btn.pack(side=tk.LEFT, padx=(0, 5))

        self.load_config_btn = ttk.Button(button_frame, text="Reload Config", command=self.load_config)
        self.load_config_btn.pack(side=tk.LEFT, padx=(0, 5))

        self.auto_sync_var = tk.BooleanVar(value=True)
        self.auto_sync_checkbox = ttk.Checkbutton(button_frame, text="Auto Sync", variable=self.auto_sync_var, command=self.toggle_auto_sync)
        self.auto_sync_checkbox.pack(side=tk.LEFT, padx=(20, 5))

        # Status frame
        status_frame = ttk.Frame(main_frame)
        status_frame.pack(fill=tk.X, pady=5)

        ttk.Label(status_frame, text="Status:").pack(side=tk.LEFT, padx=(0, 5))
        self.status_label = ttk.Label(status_frame, text="Idle")
        self.status_label.pack(side=tk.LEFT)

        ttk.Label(status_frame, text="Last Sync:").pack(side=tk.LEFT, padx=(20, 5))
        self.last_sync_label = ttk.Label(status_frame, text="Never")
        self.last_sync_label.pack(side=tk.LEFT)

        # Log area
        log_frame = ttk.LabelFrame(main_frame, text="Logs")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.log_area = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, state="disabled")
        self.log_area.pack(fill=tk.BOTH, expand=True)

        # Register the log handler
        self.log_handler = TextHandler(self.log_area)
        self.log_handler.setLevel(logging.INFO)
        self.logger.addHandler(self.log_handler)

    def load_config(self):
        try:
            if not os.path.exists("config.ini"):
                self.create_default_config()

            self.config = configparser.ConfigParser()
            self.config.read("config.ini")

            # Load URL from config
            if "API" in self.config and "url" in self.config["API"]:
                self.url_entry.delete(0, tk.END)
                self.url_entry.insert(0, self.config["API"]["url"])

            # Load last sync time
            if "SYNC" in self.config and "last_sync" in self.config["SYNC"]:
                last_sync_str = self.config["SYNC"]["last_sync"]
                if last_sync_str != "Never":
                    self.last_sync_time = datetime.datetime.fromisoformat(last_sync_str)
                    self.last_sync_label.config(text=last_sync_str)

            self.logger.info("Config loaded successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            messagebox.showerror("Error", f"Failed to load config: {str(e)}")
            return False

    def create_default_config(self):
        config = configparser.ConfigParser()

        config["DATABASE"] = {"server": "localhost", "database": "your_database", "table": "your_table", "username": "your_username", "password": "your_password"}

        config["API"] = {"url": "https://api.example.com/sync", "agent_name": "sync_agent_1", "api_token": "your_api_token"}

        config["SYNC"] = {"interval_minutes": "30", "last_sync": "Never"}

        with open("config.ini", "w") as f:
            config.write(f)

        self.logger.info("Created default config file. Please update with your settings.")

    def save_last_sync_time(self):
        if not self.config:
            return

        now = datetime.datetime.now()
        self.last_sync_time = now
        self.last_sync_label.config(text=now.isoformat(timespec="seconds"))

        if "SYNC" not in self.config:
            self.config["SYNC"] = {}

        self.config["SYNC"]["last_sync"] = now.isoformat(timespec="seconds")

        with open("config.ini", "w") as f:
            self.config.write(f)

    def get_db_connection(self):
        if not self.config:
            raise Exception("Configuration not loaded")

        server = self.config["DATABASE"]["server"]
        database = self.config["DATABASE"]["database"]
        username = self.config["DATABASE"].get("username")
        password = self.config["DATABASE"].get("password")

        if username and password:
            # Trường hợp có username/password
            connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};" f"SERVER={server};" f"DATABASE={database};" f"UID={username};" f"PWD={password}"
        else:
            # Trường hợp không có username/password => Dùng Trusted_Connection
            connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};" f"SERVER={server};" f"DATABASE={database};" "Trusted_Connection=yes"

        return pyodbc.connect(connection_string)

    def get_data_from_db(self, from_date=None):
        try:
            self.logger.info("Connecting to database...")
            conn = self.get_db_connection()
            cursor = conn.cursor()

            table = self.config["DATABASE"]["table"]

            if from_date:
                self.logger.info(f"Getting data since {from_date.isoformat()}")
                query = f"SELECT * FROM {table} WHERE date_time > ? ORDER BY date_time"
                cursor.execute(query, (from_date,))
            else:
                self.logger.info("Getting all data")
                query = f"SELECT TOP 1000 * FROM {table} ORDER BY date_time"
                cursor.execute(query)

            columns = [column[0] for column in cursor.description]
            data = []

            for row in cursor.fetchall():
                data.append(dict(zip(columns, row)))

            conn.close()
            self.logger.info(f"Retrieved {len(data)} records from database")
            return data
        except Exception as e:
            self.logger.error(f"Database error: {str(e)}")
            raise

    def sync_data(self):
        threading.Thread(target=self._sync_data, daemon=True).start()

    def _sync_data(self):
        try:
            self.status_label.config(text="Syncing...")

            if not self.config:
                raise Exception("Configuration not loaded")

            # Get API URL from text field (in case it was changed)
            api_url = self.url_entry.get().strip()
            if not api_url:
                api_url = self.config["API"]["url"]

            # Get data since last sync
            if self.last_sync_time:
                data = self.get_data_from_db(self.last_sync_time)
            else:
                data = self.get_data_from_db()

            if not data:
                self.logger.info("No new data to sync")
                self.status_label.config(text="Idle")
                return

            # Prepare headers with token
            headers = {"Content-Type": "application/json", "Authorization": f'Bearer {self.config["API"]["api_token"]}'}

            # Add agent name to each record
            agent_name = self.config["API"]["agent_name"]
            serializable_data = []
            for record in data:
                serializable_record = {}
                serializable_record["agent_name"] = agent_name

                # Convert datetime objects to strings for JSON serialization
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

            # Send data to API
            self.logger.info(f"Sending {len(data)} records to API at {api_url}")
            # append json_data to data object
            json_data = {"data": serializable_data}
            json_data_str = json.dumps(json_data)

            response = requests.post(api_url, headers=headers, data=json_data_str)

            if response.status_code == 200:
                self.logger.info(f"Sync successful. Status code: {response.status_code}")
                self.save_last_sync_time()
            else:
                self.logger.error(f"Sync failed. Status code: {response.status_code}, Response: {response.text}")

            self.status_label.config(text="Idle")
        except Exception as e:
            self.logger.error(f"Sync error: {str(e)}")
            self.status_label.config(text="Error")
            messagebox.showerror("Sync Error", str(e))

    def full_sync(self):
        self.last_sync_time = None
        self.last_sync_label.config(text="Never")
        threading.Thread(target=self._sync_data, daemon=True).start()

    def toggle_auto_sync(self):
        if self.auto_sync_var.get():
            self.start_auto_sync()
        else:
            self.stop_auto_sync()

    def start_auto_sync(self):
        if self.auto_sync_running:
            return

        if not self.config:
            return

        try:
            interval_minutes = int(self.config["SYNC"]["interval_minutes"])
            self.auto_sync_running = True
            self.auto_sync_thread = threading.Thread(target=self._auto_sync_worker, args=(interval_minutes,), daemon=True)
            self.auto_sync_thread.start()
            self.logger.info(f"Auto sync started with {interval_minutes} minute interval")
        except Exception as e:
            self.logger.error(f"Failed to start auto sync: {str(e)}")

    def stop_auto_sync(self):
        self.auto_sync_running = False
        self.logger.info("Auto sync stopped")

    def _auto_sync_worker(self, interval_minutes):
        while self.auto_sync_running:
            time.sleep(interval_minutes * 60)
            if self.auto_sync_running:  # Check again after sleep
                self._sync_data()

    def on_closing(self):
        self.stop_auto_sync()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = SyncAgent(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()
