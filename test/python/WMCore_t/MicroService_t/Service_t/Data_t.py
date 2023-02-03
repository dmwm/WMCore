"""
Unit tests for Service/Data.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""

# system modules
import unittest

# third party modules
import cherrypy

# WMCore modules
from WMCore.MicroService.Service.Data import mspileupError


class ServiceData(unittest.TestCase):
    "Unit test for Service/Data module"

    def testMSPileupError(self):
        "test mspileupError function"
        doc = {'error': 'mspileup error', 'code': 123, 'message': 'msg'}
        with self.assertRaises(cherrypy.HTTPError):
            mspileupError(doc)
        success = {}
        mspileupError(success)


if __name__ == '__main__':
    unittest.main()
