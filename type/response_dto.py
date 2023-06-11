from type.response_status import ResponseStatus


class GetAverageTempResponseDTO:

    def __init__(self, response_status: ResponseStatus, average_temp: float = None):
        self.response_status = response_status
        self.average_temp = average_temp


class GetSumKwhResponseDTO:

    def __init__(self, response_status: ResponseStatus, sum_kwh: int = None):
        self.response_status = response_status
        self.sum_kwh = sum_kwh


class GetMaxPressureResponseDTO:

    def __init__(self, response_status: ResponseStatus, max_pressure: float = None):
        self.response_status = response_status
        self.max_pressure = max_pressure
