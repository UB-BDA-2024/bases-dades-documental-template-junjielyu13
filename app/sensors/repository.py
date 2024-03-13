from fastapi import HTTPException
from sqlalchemy.orm import Session
import json
from typing import List, Optional
from . import models, schemas, last_data

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate,  mongodb_client: Session) -> models.Sensor:
    # create a new sensor in postgresql
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    # create a new sensor in mongodb
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    mydoc = {
            "sensor_id": db_sensor.id,
            "location": {
                "type": "Point",
                "coordinates": [sensor.longitude, sensor.latitude]
            },
            "type": sensor.type,
            "mac_address": sensor.mac_address,
            "manufacturer": sensor.manufacturer,
            "model": sensor.model,
            "serie_number": sensor.serie_number,
            "firmware_version": sensor.firmware_version
        }
    mongodb_client.insertOne(mydoc)

    return db_sensor

def record_data(redis: Session, sensor_id: int, data: schemas.SensorData, db: Session, mongodb_client: Session) -> schemas.Sensor:
   # get sensor from postgresql with sensor_id
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if not db_sensor:
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    # conver db_sensor to json format let it store in redis
    sensor_json = json.dumps(data.dict())
    redis.set(f"sensor-{sensor_id}", sensor_json)

    # get sensor from mongodb with sensor_id
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    col_sensor = mongodb_client.findOne({"sensor_id": sensor_id})
    if not col_sensor:
        raise HTTPException(status_code=404, detail="Sensor not found")

    # if sensor exists, update the last data in redis
    sensor = schemas.Sensor(id=sensor_id, 
                            name=db_sensor.name, 
                            latitude=col_sensor["location"]["coordinates"][0], 
                            longitude=col_sensor["location"]["coordinates"][1],
                            type=col_sensor["type"], 
                            mac_address=col_sensor["mac_address"],
                            joined_at=db_sensor.joined_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), 
                            temperature=data.temperature,
                            velocity=data.velocity, 
                            humidity=data.humidity,
                            battery_level=data.battery_level, 
                            last_seen=data.last_seen)
    return sensor

def get_data(redis: Session, sensor_id: int, db: Session, mongodb_client: Session) -> schemas.Sensor:

    # get sensor from postgresql with sensor_id
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if not db_sensor:
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    # get sensor from redis with sensor_id
    redis_sensor = redis.get(f"sensor-{sensor_id}")
    if not redis_sensor:
        raise HTTPException(status_code=404, detail="Sensor not found")
    # convert redis sensor byte format to json format
    redis_sensor = schemas.SensorData.parse_raw(redis_sensor)

    # get sensor from mongodb with sensor_id
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    mongodb_sensor = mongodb_client.findOne({"sensor_id": sensor_id})

    sensor = schemas.Sensor(id=sensor_id, name=db_sensor.name, 
                            latitude=mongodb_sensor["location"]["coordinates"][0], 
                            longitude=mongodb_sensor["location"]["coordinates"][1],
                            type=mongodb_sensor["type"], 
                            mac_address=mongodb_sensor["mac_address"],
                            joined_at=db_sensor.joined_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), 
                            temperature=redis_sensor.temperature, 
                            velocity=redis_sensor.velocity,
                            humidity=redis_sensor.humidity, 
                            battery_level=redis_sensor.battery_level, 
                            last_seen=redis_sensor.last_seen)

    return sensor

def get_sensors_near(latitude: float, longitude: float, radius: int, db: Session, mongodb_client: Session, redis_client: Session) -> list[schemas.Sensor]: 
    
    # get sensors from mongodb with sensor_id
    mongodb_client.getDatabase("sensors")
    collection = mongodb_client.getCollection("sensorsCol")

    # find sensors near to a given location
    collection.create_index([("location", "2dsphere")])
    geoJSON = {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                },
                "$maxDistance": radius
            }
        }
    }
    nearby_sensors = list(mongodb_client.findAllDocuments(geoJSON))
    sensors = []

    # find all sensors near to a given location
    for doc in nearby_sensors:
        doc["_id"] = str(doc["_id"])
        sensor = get_data(redis=redis_client, sensor_id=doc["sensor_id"], db=db, mongodb_client=mongodb_client)
        if sensor:
            sensors.append(sensor)
    
    if not sensors :
        return []
    
    return sensors

def delete_sensor(db: Session, sensor_id: int, mongodb_client: Session):

    # delete sensor from postgresql with sensor_id
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")

    db.delete(db_sensor)
    db.commit()

    # delete sensor from mongodb with sensor_id
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    mongodb_client.deleteOne({"sensor_id": sensor_id})

    return db_sensor