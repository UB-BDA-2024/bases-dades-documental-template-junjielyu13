from pydantic import BaseModel

class SensorData(BaseModel):
    velocity: float
    temperature: float
    humidity: float
    battery_level: float
    last_seen: str