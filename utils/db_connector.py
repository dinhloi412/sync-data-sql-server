import pyodbc
import logging
import datetime


class DatabaseConnector:

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def get_connection(self):
        if not self.config:
            raise Exception("Configuration not loaded")

        server = self.config["DATABASE"]["server"]
        database = self.config["DATABASE"]["database"]
        username = self.config["DATABASE"].get("username")
        password = self.config["DATABASE"].get("password")
        driver = self.config["DATABASE"].get("driver", "ODBC Driver 17 for SQL Server")

        if username and password:
            connection_string = (
                f"DRIVER={{{driver}}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"UID={username};"
                    f"PWD={password};"
                    "Encrypt=yes;"
                    "TrustServerCertificate=yes;"
                )
        else:
            connection_string = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    "Trusted_Connection=yes;"
                    "Encrypt=yes;"
                    "TrustServerCertificate=yes;"
                )
        return pyodbc.connect(connection_string)

    def get_data(self, from_date=None):
        try:
            self.logger.info("Connecting to database...")
            conn = self.get_connection()
            cursor = conn.cursor()

            table = self.config["DATABASE"]["table"]
            batch_size = int(self.config["SYNC"].get("batch_size", "1000"))

            if from_date:
                self.logger.info(f"Getting data since {from_date.isoformat()}")
                query = f"SELECT TOP {batch_size} * FROM {table} WHERE date_time > ? ORDER BY date_time"
                cursor.execute(query, (from_date,))
            else:
                self.logger.info(f"Getting latest {batch_size} records")
                query = f"SELECT TOP {batch_size} * FROM {table} ORDER BY date_time"
                cursor.execute(query)

            columns = [column[0] for column in cursor.description]
            data = []

            for row in cursor.fetchall():
                data.append(dict(zip(columns, row)))

            conn.close()
            self.logger.info(f"Retrieved {len(data)} records from database")

            last_timestamp = None
            if data and "date_time" in data[-1]:
                last_timestamp = data[-1]["date_time"]

            return data, last_timestamp
        except Exception as e:
            self.logger.error(f"Database error: {str(e)}")
            raise
