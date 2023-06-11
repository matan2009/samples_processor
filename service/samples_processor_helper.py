import numpy as np

import os

from configurations.samples_processor_configurations import SamplesProcessorConfigurations
from logging import Logger
from service.database_helper import DatabaseHelper


class SamplesProcessorHelper(SamplesProcessorConfigurations):

    def __init__(self, logger: Logger):
        super().__init__()
        self.logger = logger
        self.database_helper = DatabaseHelper()
        self.log_file_name = self.config["samples_processor_helper"]["log_file_name"]

    @staticmethod
    def calc_average_temp(samples) -> float:
        total_temp = 0
        for sample in samples:
            temp_value = float(sample[3])
            total_temp += temp_value
        avg_temp = total_temp / len(samples)
        return avg_temp

    @staticmethod
    def calc_sum_kwh(samples) -> int:
        total_kwh = 0
        for sample in samples:
            kwh_value = int(sample[1])
            total_kwh += kwh_value
        return total_kwh

    @staticmethod
    def calc_max_pressure(samples) -> float:
        max_pressure = 0
        for sample in samples:
            pressure_value = float(sample[2])
            if pressure_value > max_pressure:
                max_pressure = pressure_value
        return max_pressure

    @staticmethod
    def processed_samples(sub_list: list) -> list:
        processed_samples = []
        for sample in sub_list:
            sample = sample.rstrip("\n")
            samples_fields = sample.split("\t")
            timestamp, kwh, pressure, temp = (samples_fields[0], samples_fields[1], samples_fields[2], samples_fields[3])
            processed_samples.append((timestamp, kwh, pressure, temp))
        return processed_samples

    def database_setup(self):
        conn, cursor = self.database_helper.create_connection_to_db()
        self.database_helper.verify_table(conn, cursor)
        return conn, cursor

    def verify_log_file(self) -> str:
        if not os.path.isfile(self.log_file_name):
            raise ValueError("samples file wasn't found")
        return self.log_file_name

    def split_lines_to_sublists(self, lines: list) -> list:
        # placeholder for choosing the num of sublists dynamically according to file size
        num_of_sublists = self.config["samples_processor_helper"]["num_of_sublists"]
        return np.array_split(lines, num_of_sublists)

    def update_database_caller(self, conn, cursor, processed_samples):
        self.database_helper.update_database(conn, cursor, processed_samples)

    def fetch_samples_from_database_caller(self, start_time: str, end_time: str) -> list or None:
        try:
            samples = self.database_helper.fetch_samples_from_database(start_time, end_time)
        except Exception as ex:
            extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
            self.logger.error(f"an error occurred while trying to fetch samples from database",
                              extra={"extra": extra_msg})
            # placeholder for retry mechanism
            return None
        if not samples:
            extra_msg = f"start time is: {start_time}, end time is {end_time}"
            self.logger.error("no results returned from database", extra={"extra": extra_msg})
            return None

        return samples
