from fastapi import FastAPI
import uvicorn as uvicorn

from monitoring.logger import Logger
from service.samples_processor_helper import SamplesProcessorHelper
from type.request_dto import RequestDTO
from type.response_dto import GetAverageTempResponseDTO, GetSumKwhResponseDTO, GetMaxPressureResponseDTO
from type.response_status import ResponseStatus

app = FastAPI()

# placeholder to make the system to be asynchronous


@app.get('/average_temp')
def average_temp(request_dto: RequestDTO):
    # placeholder for input validation (fields are in the correct format for example)
    start_time = request_dto.start_time
    end_time = request_dto.end_time
    return samples_processor.get_average_temp(start_time, end_time)


@app.get('/sum_kwh')
def sum_kwh(request_dto: RequestDTO):
    # placeholder for input validation (fields are in the correct format for example)
    start_time = request_dto.start_time
    end_time = request_dto.end_time
    return samples_processor.get_sum_kwh(start_time, end_time)


@app.get('/max_pressure')
def max_pressure(request_dto: RequestDTO):
    # placeholder for input validation (fields are in the correct format for example)
    start_time = request_dto.start_time
    end_time = request_dto.end_time
    return samples_processor.get_max_pressure(start_time, end_time)


class SamplesProcessor:
    def __init__(self, logger: Logger):
        super().__init__()
        self.logger = logger.logger
        self.samples_processor_helper = SamplesProcessorHelper(self.logger)

    def set_samples_in_database(self, conn, cursor):
        try:
            log_file_name = self.samples_processor_helper.verify_log_file()
        except ValueError as ex:
            extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
            self.logger.error("samples logs file is not exist", extra={"extra": extra_msg})
            conn.close()
            return None

        with open(log_file_name, "r", encoding="utf-16") as file:
            # placeholder for reading lines in chunks
            # placeholder for reading lines using multiple workers (using ThreadPoolExecutor)
            lines = file.readlines()[1:]
        sub_lists = self.samples_processor_helper.split_lines_to_sublists(lines)
        for sub_list in sub_lists:
            try:
                # placeholder for samples processing using multiple workers (using ThreadPoolExecutor)
                processed_samples = self.samples_processor_helper.processed_samples(sub_list)
                self.samples_processor_helper.update_database_caller(conn, cursor, processed_samples)
            except Exception as ex:
                extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
                self.logger.error("failed to update sub list in database", extra={"extra": extra_msg})
                # placeholder for retry mechanism
        conn.close()

    def get_average_temp(self, start_time, end_time) -> GetAverageTempResponseDTO:
        extra_msg = f"start time is: {start_time}, end time is {end_time}"
        self.logger.info("got a request to get average temp", extra={"extra": extra_msg})
        samples = self.samples_processor_helper.fetch_samples_from_database_caller(start_time, end_time)
        if not samples:
            return GetAverageTempResponseDTO(ResponseStatus.Error)
        # placeholder to send samples to queue
        # placeholder for async sleep time
        # placeholder to get samples from queue
        avg_temp = self.samples_processor_helper.calc_average_temp(samples)
        return GetAverageTempResponseDTO(ResponseStatus.Ok, avg_temp)

    def get_sum_kwh(self, start_time, end_time) -> GetSumKwhResponseDTO:
        extra_msg = f"start time is: {start_time}, end time is {end_time}"
        self.logger.info("got a request to get sum kwh", extra={"extra": extra_msg})
        samples = self.samples_processor_helper.fetch_samples_from_database_caller(start_time, end_time)
        if not samples:
            return GetSumKwhResponseDTO(ResponseStatus.Error)
        # placeholder to send samples to queue
        # placeholder for async sleep time
        # placeholder to get samples from queue
        sum_kwh = self.samples_processor_helper.calc_sum_kwh(samples)
        return GetSumKwhResponseDTO(ResponseStatus.Ok, sum_kwh)

    def get_max_pressure(self, start_time, end_time) -> GetMaxPressureResponseDTO:
        extra_msg = f"start time is: {start_time}, end time is {end_time}"
        self.logger.info("got a request to get max pressure", extra={"extra": extra_msg})
        samples = self.samples_processor_helper.fetch_samples_from_database_caller(start_time, end_time)
        if not samples:
            return GetMaxPressureResponseDTO(ResponseStatus.Error)
        # placeholder to send samples to queue
        # placeholder for async sleep time
        # placeholder to get samples from queue
        max_pressure = self.samples_processor_helper.calc_max_pressure(samples)
        return GetMaxPressureResponseDTO(ResponseStatus.Ok, max_pressure)


if __name__ == '__main__':
    logger = Logger()
    samples_processor = SamplesProcessor(logger)
    conn, cursor = samples_processor.samples_processor_helper.database_setup()
    samples_processor.set_samples_in_database(conn, cursor)
    uvicorn.run(app, host="0.0.0.0", port=8000)
