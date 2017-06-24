#!/usr/bin/python
"""
_WMException_t_

General test for WMException

"""
from __future__ import print_function, division
import logging
import unittest
import os
from WMCore.WMBase import getTestBase

from WMCore.WMException import WMException, listWMExceptionStr

class WMExceptionTest(unittest.TestCase):
    """
    A test of a generic exception class
    """
    def setUp(self):
        """
        setup log file output.
        """
        logging.basicConfig(level=logging.NOTSET,
            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
            datefmt='%m-%d %H:%M',
            filename='%s.log' % __file__,
            filemode='w')

        self.logger = logging.getLogger('WMExceptionTest')


    def tearDown(self):
        """
        nothing to tear down
        """

        pass

    def testException(self):
        """
        create an exception and do some tests.
        """

        exception = WMException("an exception message with nr. 100", 100)
        self.logger.debug("String version of exception: " + str(exception))
        self.logger.debug("XML version of exception: " + exception.xml())
        self.logger.debug("Adding data")
        data = {}
        data['key1'] = 'value1'
        data['key2'] = 'data2'
        exception.addInfo(**data)
        self.logger.debug("String version of exception: "+ str(exception))

    def testExceptionFilter(self):
        """
        Test getWMExceptionStr function.
        """
        count = 0
        logPath = os.path.join(getTestBase(), "WMCore_t/test_condor.log")
        for result in listWMExceptionStr(logPath):
            # print(result)
            count += 1

        self.assertEqual(count, 4)

if __name__ == "__main__":
    unittest.main()
