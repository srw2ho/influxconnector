import unittest
import json
import os
from influxconnector.convertor.ppmp.v3.machine import PPMPMachine

class TestPPMPMachine(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestPPMPMachine, self).__init__(*args, **kwargs)

        with open('./test/influxconnector/convertor/ppmp/v3/examples/machine_message.json') as json_file:
            self.payload = json_file.read()


    def test_export_to_line_protocol(self):
        machine = PPMPMachine(self.payload)
        self.assertIsNotNone(machine)

        line_protocol = machine.export_to_line_protocol()
        self.assertIsNotNone(line_protocol)

        self.assertEqual(len(line_protocol.split('\n')), 2)

        line1 = line_protocol.split('\n')[0].split(',')
        self.assertEqual(line1[0], 'test-hostname')
        self.assertEqual(line1[1], 'device.hostname=test-hostname')

        point = line1[2].split(' ')
        self.assertEqual(point[0], 'device.state=OK|INFO|ERROR')
        self.assertEqual(point[1], 'hostname="test-hostname"')
        self.assertEqual(point[2], '1553150910977999872')


if __name__ == '__main__':
    unittest.main()