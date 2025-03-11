import logging
import tkinter as tk


class ColoredTextHandler(logging.Handler):

    COLORS = {logging.DEBUG: "gray", logging.INFO: "blue", logging.WARNING: "orange", logging.ERROR: "red", logging.CRITICAL: "red"}

    def __init__(self, text_widget):
        logging.Handler.__init__(self)
        self.text_widget = text_widget

        for level, color in self.COLORS.items():
            self.text_widget.tag_configure(f"level_{level}", foreground=color)

        self.text_widget.tag_configure("timestamp", foreground="green")

        self.text_widget.tag_configure("message_type", foreground="purple")

    def emit(self, record):
        msg = self.format(record)

        def append():
            self.text_widget.configure(state="normal")
            parts = msg.split(" - ", 2)
            if len(parts) >= 3:
                timestamp, level, content = parts
                self.text_widget.insert(tk.END, timestamp + " - ", "timestamp")
                self.text_widget.insert(tk.END, level + " - ", "message_type")
                self.text_widget.insert(tk.END, content + "\n", f"level_{record.levelno}")
            elif len(parts) == 2:
                timestamp, content = parts
                self.text_widget.insert(tk.END, timestamp + " - ", "timestamp")
                self.text_widget.insert(tk.END, content + "\n", f"level_{record.levelno}")
            else:
                self.text_widget.insert(tk.END, msg + "\n", f"level_{record.levelno}")

            self.text_widget.configure(state="disabled")
            self.text_widget.yview(tk.END)

        self.text_widget.after(0, append)
