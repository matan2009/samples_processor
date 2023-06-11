from pydantic.main import BaseModel


class RequestDTO(BaseModel):
    start_time: str
    end_time: str
