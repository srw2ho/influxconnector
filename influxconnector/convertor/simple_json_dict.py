class SimpleJSONDict(object):
    """ Class that represents a simple JSON Dictionary

    Arguments:
        object {[type]} -- [description]
    """

    def __init__(self, json):
        self._data = json


    def export_to_influxdb(self, measurement):
        """ Export object information to InfluxDB json format
        """
        data = []

        for key, value in self._data.items():
            data.append(
                {
                    "measurement": measurement,
                    "time": value['timestamp'],
                    "fields": {
                        key: value['value']
                    }
                }
            )

        return data
