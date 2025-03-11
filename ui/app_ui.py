import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import logging
import datetime
from utils.log_handler import ColoredTextHandler


class SyncAgentUI:
    def __init__(self, root, sync_handler, full_sync_handler):
        self.root = root
        self.sync_handler = sync_handler
        self.full_sync_handler = full_sync_handler

        self.logger = logging.getLogger(__name__)

        self.setup_ui()

    def setup_ui(self):
        self.root.title("SQL Server Sync Agent")
        self.root.geometry("800x600")

        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        url_frame = ttk.Frame(main_frame)
        url_frame.pack(fill=tk.X, pady=5)

        ttk.Label(url_frame, text="API URL:").pack(side=tk.LEFT, padx=(0, 5))
        self.url_entry = ttk.Entry(url_frame, width=50)
        self.url_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=5)

        self.sync_btn = ttk.Button(button_frame, text="Sync Data", command=self.sync_handler)
        self.sync_btn.pack(side=tk.LEFT, padx=(0, 5))

        self.full_sync_btn = ttk.Button(button_frame, text="Full Sync", command=self.full_sync_handler)
        self.full_sync_btn.pack(side=tk.LEFT, padx=(0, 5))

        status_frame = ttk.Frame(main_frame)
        status_frame.pack(fill=tk.X, pady=5)

        ttk.Label(status_frame, text="Status:").pack(side=tk.LEFT, padx=(0, 5))
        self.status_label = ttk.Label(status_frame, text="Idle")
        self.status_label.pack(side=tk.LEFT)

        ttk.Label(status_frame, text="Last Sync:").pack(side=tk.LEFT, padx=(20, 5))
        self.last_sync_label = ttk.Label(status_frame, text="Never")
        self.last_sync_label.pack(side=tk.LEFT)

        log_frame = ttk.LabelFrame(main_frame, text="Logs")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.log_area = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, state="disabled")
        self.log_area.pack(fill=tk.BOTH, expand=True)

        self.log_handler = ColoredTextHandler(self.log_area)
        self.log_handler.setLevel(logging.INFO)
        self.log_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        root_logger = logging.getLogger()
        root_logger.addHandler(self.log_handler)

    def set_url(self, url):
        self.url_entry.delete(0, tk.END)
        self.url_entry.insert(0, url)

    def get_url(self):
        return self.url_entry.get().strip()

    def update_status(self, status):
        self.status_label.config(text=status)

    def update_last_sync(self, timestamp):
        if timestamp:
            if isinstance(timestamp, str):
                self.last_sync_label.config(text=timestamp)
            else:
                self.last_sync_label.config(text=timestamp.isoformat(timespec="seconds"))
        else:
            self.last_sync_label.config(text="Never")

    def set_buttons_state(self, enabled):
        state = tk.NORMAL if enabled else tk.DISABLED
        self.sync_btn.config(state=state)
        self.full_sync_btn.config(state=state)

    def show_error(self, title, message):
        messagebox.showerror(title, message)
