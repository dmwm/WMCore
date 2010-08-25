#from MessageService.MessageService import MessageService
#from WMComponent_t.HTTPFrontend_t.MsgServiceApp import MsgServiceApp

import os

import unittest
import threading


from WMCore.WMFactory import WMFactory
from WMCore.Database.Transaction import Transaction
from WMComponent.HTTPFrontend.HTTPFrontEnd import HTTPFrontEnd

class MsgServiceApp(unittest.TestCase):
    """
    TestCase for modules
    """
    def __init__(self, name):
        unittest.TestCase.__init__()
        self.name = name
        self._maxMessage = 10



    def setUp(self):
        """
        setup for test.
        """
        
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = 
                                ["WMCore.ThreadPool",
                                 "WMCore.MsgService",
                                 ],
                                useDefault = False)


    def msgService(self):
        """ returns a MsgService instance """
        # load a message service as we want to check if total failure
        # messages are returned

        myThread = threading.currentThread()
        myThread.transaction = Transaction(myThread.dbi)
        myThread.transaction.begin()
        factory = WMFactory('msgService', 'WMCore.MsgService.'+myThread.dialect)
        msgService =  factory.loadObject("MsgService")
        msgService.registerAs(self.name)
        # subscribe to what we want to test.
        myThread.transaction.commit()
        return msgService

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase()
    def testA(self):
        config = self.testInit.getConfiguration(os.path.join(os.getenv('WMCOREBASE'), \
                        'src/python/WMComponent/ErrorHandler/DefaultConfig.py'))
        self.testInit.generateWorkDir(config)
        
        harness = HTTPFrontEnd(config)
        harness.prepareToStart()
        harness.handleMessage("HTTPFrontendStart", "GOGOGOGOGO")

    def testB(self):
        ms = self.msgService()
        
        myThread = threading.currentThread()
        myThread.transaction = Transaction(myThread.dbi)
        myThread.transaction.begin()
        ms.subscribeTo("HTTPFrontendStart")
        
        ms.purgeMessages()
        ms.publish({'name' : 'HTTPFrontendStart', 'payload' : 'GO', 'delay' : '00:00:00', 'instant' : True})
        myThread.transaction.commit()
        ms.finish()
        
        myThread.transaction.begin()
        #print str(ms.pendingMsgs())
        myThread.transaction.commit()
        
        

