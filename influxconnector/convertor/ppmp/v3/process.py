import json
from dateutil import parser
from datetime import datetime, timedelta
from influxdb import line_protocol
from functools import reduce

class PPMPProcess(object):
    """ Class that represents a PPMP Process Payload packet

    Arguments:
        object {[type]} -- [description]
    """

    def __init__(self, payload):
        if isinstance(payload, dict):
            self.data = payload
        else:
            self.data = json.loads(payload)

        # InfluxDB Tags
        self.tags = {}
        # InfluxDB Points
        self.points = []
        # Line Protocol Output
        self.line_protocol_data = ""

    def hostname(self):
        """ Retrieve device hostname

        Returns:
            [str] -- hostname or device.id
        """
        try:
            return self.data['device']['additionalData']['hostname']
        except KeyError:
            return self.data['device']['id']

    def export_to_line_protocol(self):
        """ Export object to InfluxDB Line Protocol syntax
        """
        """data = {
            "tags": {
                "empty_tag": "",
                "none_tag": None,
                "backslash_tag": "C:\\",
                "integer_tag": 2,
                "string_tag": "hello"
            },
            "points": [
                {
                    "measurement": "test",
                    "fields": {
                        "string_val": "hello!",
                        "int_val": 1,
                        "float_val": 1.1,
                        "none_field": None,
                        "bool_val": True,
                    },
                    "time": 0
                }
            ]
        }"""
        # Device
        self.add_tag(['device', 'mode'])
        self.add_tag(['device', 'state'])

        self.add_tags(['device', 'additionalData'], prefix=['device'])

        # Part
        self.add_tag(['part', 'id'])
        self.add_tag(['part', 'type'])
        self.add_tag(['part', 'typeId'])
        self.add_tag(['part', 'code'])
        self.add_tag(['part', 'result'])

        self.add_tags(['part', 'additionalData'], prefix=['part'])

        # TODO: Process

        # ProcessMeasurements
        for measurement in self.data['measurements']:
            # create new Measurement object to store local tags
            meas_obj = Measurement(json.dumps(measurement), self.hostname())

            meas_obj.add_tag(['code'], ['process'])
            meas_obj.add_tag(['name'], ['process'])
            meas_obj.add_tag(['phase'], ['process'])
            meas_obj.add_tag(['result'], ['process'])

            meas_obj.add_tags(['additionalData'], prefix=['process'])

            # TODO: context
            # TODO: specialValues

            timestamp = parser.parse(measurement['ts'])
            keys = list(filter(lambda key: key != 'time', measurement['series'].keys()))

            self.points = []
            for index in range(0, len(measurement['series'][keys[0]])):
                fields = {key.replace(' ', '_'): measurement['series'][key][index] for key in keys if key != '' and measurement['series'][key][index] != ''}

                # add offset to base timestamp
                ts_w_offset = timestamp + timedelta(milliseconds=measurement['series']['time'][index])
                # round to InfluxDB compatible timestamp (nanoseconds)
                ts_w_offset = int(round(ts_w_offset.timestamp() * 1000000000))

                self.add_point(fields, ts_w_offset)

            # merge global ProcessPayload tags with current ProcessMeasurement tags
            current_tags = self.tags.copy()
            current_tags.update(meas_obj.tags)

            # add measurement in line_protocol format
            if len(fields) > 0:
                tmp = line_protocol.make_lines({'tags': current_tags, 'points': self.points})
                self.line_protocol_data += tmp

        # return sequence of line protocol strings
        return self.line_protocol_data if self.line_protocol_data else None

    def add_tag(self, path, prefix=[]):
        """ Add tag in case it is found under the given path

        Arguments:
            path {list(str)} -- Path list (e.g. ['part', 'type'])

        Keyword Arguments:
            prefix {list(str)} -- Prefix list to add to tag name (default: [])
        """
        value = get_nested(self.data, path)
        if value is not None:
            name = '.'.join(prefix + path)
            self.tags[name] = value

    def add_tags(self, path, prefix=[]):
        """ Add tags in case that they are found under the given path (add prefix if specified)

        Arguments:
            path {list(str)} -- Path list (e.g. ['part', 'additionalData'])

        Keyword Arguments:
            prefix {list(str)} -- Prefix list to add to tag name (default: [])
        """
        var = get_nested(self.data, path)
        if var is not None:
            self.tags.update({f"{'.'.join(prefix + [str(key)])}": value for key, value in var.items()})

    def add_point(self, fields, timestamp):
        """ Add additional point to given array

        Arguments:
            var {obj} -- Variable to add to
            fields {obj} -- Fields object
            ts {str} -- Timestamp
        """
        self.points.append({
            "measurement": self.hostname(),
            "fields": fields,
            "time": timestamp
        })


def get_nested(obj, path):
    """ Deep retrieve of value at path in obj

    Arguments:
        obj {obj} -- Data object
        path {list(str)} -- Path list that describes where to retrieve the value from

    Returns:
        [obj] -- Retrieved value
    """
    return reduce(dict.get, path, obj)


class Measurement(PPMPProcess):

    def __init__(self, payload, hostname):
        super().__init__(payload)

        self.hostname = hostname

    def hostname(self):
        return self.hostname
