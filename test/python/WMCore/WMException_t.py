#!/usr/bin/python
"""
_WMException_t_

General test for WMException

"""

__revision__ = "$Id: WMException_t.py,v 1.2 2008/08/26 13:55:16 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "fvlingen@caltech.edu"

import logging
import unittest

from WMCore.WMException import WMException

class WMExceptionTest(unittest.TestCase):
    """
    A test of a generic exception class
    """    
    def setUp(self):
        """
        setup log file output.
        """
        logging.basicConfig(level=logging.DEBUG,
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
        
            
if __name__ == "__main__":
    unittest.main()     
