import sqlite3

from configurations.samples_processor_configurations import SamplesProcessorConfigurations


class DatabaseHelper(SamplesProcessorConfigurations):

    def __init__(self):
        super().__init__()
        self.database_path = self.config["database_helper"]["samples_db_path"]
        self.table_name = self.config['database_helper']['table_name']

    def create_connection_to_db(self):
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        return conn, cursor

    def verify_table(self, conn, cursor):
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table_name}';")
        if not cursor.fetchone():
            cursor.execute((f'''
            CREATE TABLE {self.table_name}(
                timestamp TEXT PRIMARY KEY,
                kwh TEXT,
                pressure TEXT,
                tepm TEXT)'''))
            conn.commit()

    def update_database(self, conn, cursor, processed_samples):
        insert_query = f"INSERT OR IGNORE INTO {self.table_name} (timestamp, kwh, pressure, tepm) VALUES (?, ?, ?, ?)"
        cursor.executemany(insert_query, processed_samples)
        conn.commit()

    def fetch_samples_from_database(self, start_time: str, end_time: str) -> list:
        conn, cursor = self.create_connection_to_db()
        fetch_query = f"SELECT * FROM {self.table_name} WHERE timestamp >= '{start_time}' AND timestamp <= '{end_time}'"
        cursor.execute(fetch_query)
        samples = cursor.fetchall()
        conn.close()
        return samples
