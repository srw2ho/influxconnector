import unittest
import json
import os
from influxconnector.convertor.ppmp.v3.measurement import PPMPMeasurement

class TestPPMPMeasurement(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestPPMPMeasurement, self).__init__(*args, **kwargs)

        with open('./test/influxconnector/convertor/ppmp/v3/examples/measurement_message.json') as json_file:
            self.payload = json_file.read()


    def test_export_to_line_protocol(self):
        measurement = PPMPMeasurement(self.payload)
        self.assertIsNotNone(measurement)

        line_protocol = measurement.export_to_line_protocol()
        self.assertIsNotNone(line_protocol)

        self.assertEqual(len(line_protocol.split('\n')), 4)

        line1 = line_protocol.split('\n')[0].split(',')
        self.assertEqual(line1[0], 'DEV-100001')
        self.assertEqual(line1[1], 'device.hostname=DEV-100001')
        self.assertEqual(line1[6], 'measurement.xy=xyz')
        self.assertEqual(line1[10], 'part.result=OK')
        self.assertEqual(line1[10], 'part.result=OK')

        point = line1[13].split(' ')
        self.assertEqual(point[0], 'part.typeId=F00V7328')
        self.assertEqual(point[1], 'temperature=45.4231')
        self.assertEqual(point[2], '1553235264180999936')


if __name__ == '__main__':
    unittest.main()