# Register all sensors
from dagster_dag_factory.sensors.s3_sensor import S3Sensor
from dagster_dag_factory.sensors.sftp_sensor import SftpSensor
from dagster_dag_factory.sensors.base_sensor import SensorRegistry, BaseSensor

__all__ = ["S3Sensor", "SftpSensor", "SensorRegistry", "BaseSensor"]
