import json
import pytz
from dateutil import parser
from datetime import datetime, timedelta
from influxdb import line_protocol
from functools import reduce


class PPMPMachine(object):
    """ Class that represents a PPMP Machine Message packet

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
        # Device
        self.add_tag(['device', 'mode'])
        self.add_tag(['device', 'state'])

        self.add_tags(['device', 'additionalData'], prefix=['device'])

        # Messages
        for message in self.data['messages']:
            # create new Measurement object to store local tags
            mess_obj = Message(json.dumps(message), self.hostname())

            mess_obj.add_tag(['code'], ['message'])
            mess_obj.add_tag(['description'], ['message'])
            mess_obj.add_tag(['hint'], ['message'])
            mess_obj.add_tag(['origin'], ['message'])
            mess_obj.add_tag(['severity'], ['message'])
            mess_obj.add_tag(['title'], ['message'])
            mess_obj.add_tag(['type'], ['message'])

            mess_obj.add_tags(['additionalData'], prefix=['message'])

            timestamp = parser.parse(message['ts'])

            # in case of a LWT ERROR message -> write current timestamp instead of initial publishing timestamp
            if 'device' in self.data and 'state' in self.data['device'] and self.data['device']['state'] == 'ERROR' and 'code' in message and message['code'] == 'offline':
                timestamp = pytz.timezone("Europe/Berlin").localize(datetime.now())

            # add hostname as field
            hostname = self.hostname if self.hostname() else ''
            if hostname:
                self.add_point({"hostname": self.hostname()}, timestamp)

            # add code as field (remove this again?)
            if message['code']:
                self.add_point({"code": message['code']}, timestamp)

            # merge global MessagePayload tags with current Message tags
            current_tags = self.tags.copy()
            current_tags.update(mess_obj.tags)

            # add message in line_protocol format
            tmp = line_protocol.make_lines({'tags': current_tags, 'points': self.points})
            self.line_protocol_data += tmp

        # return sequence of line protocol strings
        return self.line_protocol_data

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
        # TODO: only add point if value != ""

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
    try:
        return reduce(dict.get, path, obj)
    except TypeError as e:
        return None


class Message(PPMPMachine):

    def __init__(self, payload, hostname):
        super().__init__(payload)

        self.hostname = hostname

    def hostname(self):
        return self.hostname
