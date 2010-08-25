import os
import unittest
import mock
import WMCore_t.Storage_t.Plugins_t.PluginTestBase_t
import WMCore.Storage.Plugins.DCCPFNALImpl as ourPlugin
import subprocess



class DCCPFNALImplTest(unittest.TestCase):
    def setUp(self):
        pass
    
    def tearDown(self):
        pass
    
    def testWin(self):
        def monkeys(commands):
            print "I AM A MONKEY"
            return (1, 2)
            
        ourPlugin.runCommand = monkeys
        
        testObject = ourPlugin.DCCPFNALImpl()
        
        testObject.doWrapped(['echo','hi'])
        testObject.doTransfer('/etc/hosts',
                              '/tmp/testfile',
                              True, 
                              None,
                              None,
                              None,
                              None)
        testObject.doDelete('/store/tmp/testfile', None, None, None, None  )

if __name__ == "__main__":
    unittest.main()

