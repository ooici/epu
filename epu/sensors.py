import time

def sensor_message(sensor_id, value, timestamp=None):
    """Builds a sensor message
    """
    if timestamp is None:
        timestamp = long(time.time() * 1e6)

    return dict(sensor_id=str(sensor_id), time=long(timestamp), value=value)
  