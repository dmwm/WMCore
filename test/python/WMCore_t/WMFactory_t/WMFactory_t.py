#!/usr/bin/env python
"""
Test WMFactory module
"""

__revision__ = "$Id: WMFactory_t.py,v 1.1 2008/10/01 11:09:13 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"


import logging
import os
import threading
import unittest

from WMCore.WMFactory import WMFactory

class WMFactoryTest(unittest.TestCase):
    """
    TestCase for Registry module 
    """

    _setup_done = False
    _log_level = 'debug'
    # for now set the relative path
    _repository = 'WMCore_t.WMFactory_t'
    

    def setUp(self):
        
        logging.basicConfig(level=logging.DEBUG,
            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
            datefmt='%m-%d %H:%M',
            filename='%s.log' % __file__,
            filemode='w')
        
        self.logger = logging.getLogger('WMERegistryTest')
        
        if not WMFactoryTest._setup_done:
            if WMFactoryTest._log_level == 'debug':
                logging.getLogger().setLevel(logging.DEBUG)
            logging.debug("loading repositories")
            WMFactoryTest._setup_done = True

    def testRegistry(self):
        logging.debug("Initialize registry")
        # somewhere (e.g. during initialization) you create a registry for queries
        registry = WMFactory('my_test_registry', WMFactoryTest._repository)

        testRegisteredFile = \
        registry.loadObject('TestCollection_1.TestRegistryFile1')
        parameters = {'par1':'val1', 'par2':'val2'}
        result = testRegisteredFile.doSomething(parameters)
        self.assertEqual(parameters, result)
        # this time from cache
        testRegisteredFile = \
        registry.loadObject('TestCollection_1.TestRegistryFile2')
        parameters = {'par3':'val3', 'par4':'val4'}
        result = testRegisteredFile.doSomething(parameters)
        self.assertEqual(parameters, result)

        testRegisteredFile = \
        registry.loadObject('TestCollection_2.TestRegistryFile1')
        parameters = {'par5':'val5', 'par6':'val6'}
        result = testRegisteredFile.doSomething(parameters)
        self.assertEqual(parameters, result)

        testRegisteredFile = \
        registry.loadObject('TestCollection_2.TestRegistryFile2')
        parameters = {'par7':'val7', 'par8':'val8'}
        result = testRegisteredFile.doSomething(parameters)
        self.assertEqual(parameters, result)
        logging.debug("testing cache")
        testRegisteredFile = \
        registry.loadObject('TestCollection_2.TestRegistryFile2')
        testRegisteredFile = \
        registry.loadObject('TestCollection_2.TestRegistryFile2')
        testRegisteredFile = \
        registry.loadObject('TestCollection_2.TestRegistryFile2')
        logging.debug("testing multiple instances (no cache)")
        testRegisteredFileInst1 = \
        registry.loadObject('TestCollection_2.TestRegistryFile2', \
        storeInCache = False)
        testRegisteredFileInst2 = \
        registry.loadObject('TestCollection_2.TestRegistryFile2', \
        storeInCache = False)
        testRegisteredFileInst3 = \
        registry.loadObject('TestCollection_2.TestRegistryFile2', \
        storeInCache = False)
            
        parameters = {'par9':'val9', 'par10':'val10'}
        result = testRegisteredFileInst1.doSomething(parameters)
        self.assertEqual(parameters, result)
        parameters = {'par11':'val11', 'par12':'val12'}    
        result = testRegisteredFileInst2.doSomething(parameters)
        self.assertEqual(parameters, result)
        parameters = {'par13':'val13', 'par14':'val14'}
        result = testRegisteredFileInst3.doSomething(parameters)
        self.assertEqual(parameters, result)
        
        logging.debug("Retrieving registry from thread attribute and load objects")
        logging.debug("this way all objects in this thread can use the registry")
        myThread = threading.currentThread()
        registry = myThread.factory['my_test_registry']
        parameters = {'par15':'val15', 'par16':'val16'}
        testRegisteredFile = \
        registry.loadObject('TestCollection_3.TestRegistryFile1', \
        args = parameters)
        result = testRegisteredFile.doSomething(parameters)
        self.assertEqual(parameters, result)
    
    def runTest(self):
        self.testRegistry()    
if __name__ == '__main__':
    unittest.main()
