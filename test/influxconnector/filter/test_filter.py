import unittest
from influxconnector.filter.filter import simple_json_filter

TEST_DICT = {
    "_Toggle1s": {"value": True, "timestamp": "2018-03-29 13:46:14.368471"},
    "_Toggle10s": {"value": False, "timestamp": "2018-03-29 13:46:07.871320"},
    "systemstatus": {"value": 1234, "timestamp": "2018-03-29 13:46:14.368471"},
    "systemstatus_kw": {"value": 12.34, "timestamp": "2018-03-29 13:46:07.871320"},
    "RS_202_U_sen_a": {"value": 56.78, "timestamp": "2018-03-29 13:46:14.368471"},
    "RS_222_U_sen_a": {"value": 78.910, "timestamp": "2018-03-29 13:46:14.368471"},
    "RS_209_U_sen_a": {"value": 12.34, "timestamp": "2018-03-29 13:46:14.368471"},
    "RS_811_U_sen_a": {"value": 12.34, "timestamp": "2018-03-29 13:46:14.368471"}
}


class TestFilter(unittest.TestCase):

    def test_simple_json_filter(self):
        # test identity of function
        self.assertEqual(TEST_DICT, simple_json_filter(
            TEST_DICT.keys(), TEST_DICT))

        # test complete blacklist
        self.assertEqual({}, simple_json_filter([], TEST_DICT))

        # test single blacklisting
        self.assertEqual(
            {
                "_Toggle1s": {"value": True, "timestamp": "2018-03-29 13:46:14.368471"},
                "systemstatus": {"value": 1234, "timestamp": "2018-03-29 13:46:14.368471"},
                "systemstatus_kw": {"value": 12.34, "timestamp": "2018-03-29 13:46:07.871320"},
                "RS_202_U_sen_a": {"value": 56.78, "timestamp": "2018-03-29 13:46:14.368471"},
                "RS_222_U_sen_a": {"value": 78.910, "timestamp": "2018-03-29 13:46:14.368471"},
                "RS_209_U_sen_a": {"value": 12.34, "timestamp": "2018-03-29 13:46:14.368471"},
                "RS_811_U_sen_a": {"value": 12.34, "timestamp": "2018-03-29 13:46:14.368471"}
            },
            simple_json_filter(
                ['_Toggle1s', 'systemstatus', 'systemstatus_kw', 'RS_202_U_sen_a',
                 'RS_222_U_sen_a', 'RS_209_U_sen_a', 'RS_811_U_sen_a'],
                TEST_DICT
            ))

        # test single whitelisting
        self.assertEqual(
            {
                "RS_202_U_sen_a": {"value": 56.78, "timestamp": "2018-03-29 13:46:14.368471"},
            },
            simple_json_filter(
                ['RS_202_U_sen_a'],
                TEST_DICT
            ))

        # test mixed black/whitelisting
        self.assertEqual(
            {
                "_Toggle10s": {"value": False, "timestamp": "2018-03-29 13:46:07.871320"},
                "systemstatus_kw": {"value": 12.34, "timestamp": "2018-03-29 13:46:07.871320"},
                "RS_209_U_sen_a": {"value": 12.34, "timestamp": "2018-03-29 13:46:14.368471"},
                "RS_811_U_sen_a": {"value": 12.34, "timestamp": "2018-03-29 13:46:14.368471"}
            },
            simple_json_filter(
                ['_Toggle10s', 'systemstatus_kw', 'RS_209_U_sen_a', 'RS_811_U_sen_a'],
                TEST_DICT
            ))


if __name__ == '__main__':
    unittest.main()
