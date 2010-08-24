#!/usr/bin/env python
"""
Test Registry.Registry module
"""

__revision__ = "$Id: WMRegistry_t.py,v 1.1 2008/09/25 13:14:04 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"


import logging
import os
import threading
import unittest

from WMCore.WMRegistry import WMRegistry

class WMRegistryTest(unittest.TestCase):
    """
    TestCase for Registry module 
    """

    _setup_done = False
    _log_level = 'debug'
    # for now set the relative path
    _repositories = ['test/python/WMCore/WMRegistry_t']
    

    def setUp(self):
        
        logging.basicConfig(level=logging.DEBUG,
            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
            datefmt='%m-%d %H:%M',
            filename='%s.log' % __file__,
            filemode='w')
        
        self.logger = logging.getLogger('WMERegistryTest')
        
        if not WMRegistryTest._setup_done:
            if WMRegistryTest._log_level == 'debug':
                logging.getLogger().setLevel(logging.DEBUG)
                for i in xrange(0, len(WMRegistryTest._repositories)):
                    # create absolute paths
                    WMRegistryTest._repositories[i] = os.path.join(\
                        os.environ['WMCOREBASE'], \
                        WMRegistryTest._repositories[i])
            logging.debug("loading repositories")
            WMRegistryTest._setup_done = True

    def testRegistry(self):
        logging.debug("Initialize registry")
        # somewhere (e.g. during initialization) you create a registry for queries
        registry = WMRegistry('my_test_registry',WMRegistryTest._repositories)

        testRegisteredFile = registry.loadObject('TestCollection_1.TestRegistryFile1')
        parameters = {'par1':'val1','par2':'val2'}
        result = testRegisteredFile.doSomething(parameters)
        self.assertEqual(parameters,result)
        # this time from cache
        testRegisteredFile = registry.loadObject('TestCollection_1.TestRegistryFile2')
        parameters = {'par3':'val3','par4':'val4'}
        result = testRegisteredFile.doSomething(parameters)
        self.assertEqual(parameters,result)

        testRegisteredFile = registry.loadObject('TestCollection_2.TestRegistryFile1')
        parameters = {'par5':'val5','par6':'val6'}
        result = testRegisteredFile.doSomething(parameters)
        self.assertEqual(parameters,result)

        testRegisteredFile = registry.loadObject('TestCollection_2.TestRegistryFile2')
        parameters = {'par7':'val7','par8':'val8'}
        result = testRegisteredFile.doSomething(parameters)
        self.assertEqual(parameters,result)
        logging.debug("testing cache")
        testRegisteredFile = registry.loadObject('TestCollection_2.TestRegistryFile2')
        testRegisteredFile = registry.loadObject('TestCollection_2.TestRegistryFile2')
        testRegisteredFile = registry.loadObject('TestCollection_2.TestRegistryFile2')
        logging.debug("testing multiple instances (no cache)")
        testRegisteredFile_inst1 = registry.loadObject('TestCollection_2.TestRegistryFile2', cache = False)
        testRegisteredFile_inst2 = registry.loadObject('TestCollection_2.TestRegistryFile2', cache = False)
        testRegisteredFile_inst3 = registry.loadObject('TestCollection_2.TestRegistryFile2', cache = False)
            
        parameters = {'par9':'val9','par10':'val10'}
        result = testRegisteredFile_inst1.doSomething(parameters)
        self.assertEqual(parameters,result)
        parameters = {'par11':'val11','par12':'val12'}    
        result = testRegisteredFile_inst2.doSomething(parameters)
        self.assertEqual(parameters,result)
        parameters = {'par13':'val13','par14':'val14'}
        result = testRegisteredFile_inst3.doSomething(parameters)
        self.assertEqual(parameters,result)
        
        logging.debug("Retrieving registry from thread attribute and load objects")
        logging.debug("this way all objects in this thread can use the registry")
        myThread = threading.currentThread()
        registry = myThread.registries['my_test_registry']
        parameters = {'par15':'val15','par16':'val16'}
        testRegisteredFile = registry.loadObject('TestCollection_3.TestRegistryFile1', args = parameters)
        result = testRegisteredFile.doSomething(parameters)
        self.assertEqual(parameters,result)
        
if __name__ == '__main__':
    unittest.main()
