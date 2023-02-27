#!/usr/bin/env python
"""
Unittests for CPMetrics functions
"""

import unittest

from Utils.CPMetrics import flattenStats, promMetrics


class CPMetricsTests(unittest.TestCase):
    """
    unittest for CPMetrics functions
    """
    def setUp(self):
        """
        setup data to test
        """
        self.testData = {
            "Cheroot HTTPServer 4388603856": {"a": 1},
            "CherryPy Applications": {"Bytes Read/Request": 0.0}}
        return

    def tearDown(self):
        """
        Do nothing
        """
        return

    def testFlattenStats(self):
        """
        Test the flattenStats function
        """
        data = flattenStats(self.testData)
        expect = {"cherrypy_app_bytes_read_request": 0.0, "cherrypy_http_server_a": 1}
        self.assertCountEqual(data, expect)

    def testPromMetrics(self):
        """
        Test the flattenStats function
        """
        data = promMetrics(self.testData, 'test-exporter')
        self.assertEqual("# HELP" in data, True)
        self.assertEqual("# TYPE" in data, True)
        self.assertEqual("test_exporter_cherrypy_app_bytes_read_request" in data, True)
        self.assertEqual("bla-bla" in data, False)


if __name__ == '__main__':
    unittest.main()
