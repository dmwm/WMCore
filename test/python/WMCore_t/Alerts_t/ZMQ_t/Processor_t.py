#!/usr/bin/env python
# encoding: utf-8

"""
Created by Dave Evans on 2011-03-15.
Copyright (c) 2011 Fermilab. All rights reserved.

"""


import os
import time
import unittest

from multiprocessing import Process, Queue

from WMCore.Alerts.Alert import Alert
from WMCore.Configuration import Configuration
from WMCore.Alerts.ZMQ.Processor import Processor
from WMCore.Alerts.ZMQ.Sender import Sender
from WMCore.Alerts.ZMQ.Receiver import Receiver
from WMCore.Alerts.ZMQ.Sinks.FileSink import FileSink
from WMQuality.TestInitCouchApp import TestInitCouchApp



def simpleWorker(addr, ctrl):
    """
    Sender that pauses and sends a shutdown message.
    
    """
    s = Sender(addr, "Processor_t", ctrl)
    s.register()
    s.unregister()
    s.sendShutdown()
 
 
    
def worker(addr, ctrl, nAlerts, workerId = "Processor_t"):
    """
    Send a few alerts.
     
    """
    s = Sender(addr, workerId, ctrl)
    s.register()
    for i in range(0, nAlerts):
        a = Alert(Type = "Alert", Level = i)
        s(a)
    s.unregister()
    s.sendShutdown()
    
    
    
class ProcessorTest(unittest.TestCase):
    """
    TestCase for Processor.
    
    """
    
    
    def setUp(self):
        """
        Set up for tests.
        
        """
        self.addr = "tcp://127.0.0.1:5557"
        self.ctrl = "tcp://127.0.0.1:5559"
        
        self.softOutputFile = "/tmp/ProcessorTestSoftAlerts.json"
        self.criticalOutputFile = "/tmp/ProcessorTestCriticalAlerts.json"
        
        self.config = Configuration()
        self.config.component_("AlertProcessor")
        self.config.AlertProcessor.section_("critical")
        self.config.AlertProcessor.section_("soft")
        
        self.config.AlertProcessor.critical.level = 5
        self.config.AlertProcessor.soft.level = 0
        self.config.AlertProcessor.soft.bufferSize = 3
        
        self.config.AlertProcessor.critical.section_("sinks")
        self.config.AlertProcessor.soft.section_("sinks")
                        
        
    def tearDown(self):
        for f in (self.criticalOutputFile, self.softOutputFile):
            if os.path.exists(f):
                os.remove(f)
        if hasattr(self, "testInit"):
            self.testInit.tearDownCouch()
        

    def testProcessorBasic(self):
        print str(self.config.AlertProcessor)
        p = Processor(self.config.AlertProcessor)


    def testProcessorWithReceiver(self):
        """
        Test startup and shutdown of processor in receiver.
        
        """
        processor = Processor(self.config.AlertProcessor)
        rec = Receiver(self.addr, processor, self.ctrl)
        rec.startReceiver()
        # since the above is non-blocking, could send the messages from here
        # directly, yet running via Process doesn't harm
        sender = Process(target = simpleWorker, args = (self.addr, self.ctrl))
        sender.start()
        # wait until the Receiver is shut by simpleWorker
        while rec.isReady():
            time.sleep(0.1)
        
            
    def testProcessorWithReceiverAndFileSink(self):
        # add corresponding part of the configuration for FileSink(s)
        config = self.config.AlertProcessor
        config.critical.sinks.section_("file")
        config.critical.sinks.file.outputfile = self.criticalOutputFile 
        
        config.soft.sinks.section_("file")
        config.soft.sinks.file.outputfile = self.softOutputFile
        
        processor = Processor(config)
        rec = Receiver(self.addr, processor, self.ctrl)
        rec.startReceiver() # non blocking call
        
        # run worker(), this time directly without Process as above,
        # worker will send 10 Alerts to Receiver
        worker(self.addr, self.ctrl, 10)
        
        # wait until the Receiver is shut by worker
        while rec.isReady():
            time.sleep(0.1)
            
        # now check the FileSink output files for content:
        # the soft Alerts has threshold level set to 0 so Alerts
        # with level 1 and higher, resp. for critical the level
        # was above set to 5 so 6 and higher out of worker's 0 .. 9
        # (10 Alerts altogether) shall be present
        softSink = FileSink(config.soft.sinks.file)
        criticalSink = FileSink(config.critical.sinks.file)
        softList = softSink.load()
        criticalList = criticalSink.load()
        # check soft level alerts
        self.assertEqual(len(softList), 9) # levels 1 .. 9 went in
        for a, level in zip(softList, range(1, 9)):
            self.assertEqual(a["Level"], level)
        # check 'critical' levels
        self.assertEqual(len(criticalList), 4) # only levels 6 .. 9 went in
        for a, level in zip(criticalList, range(6, 9)):
            self.assertEqual(a["Level"], level)
            
            
    def testProcessorWithReceiverAndCouchSink(self):
        # set up couch first
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        dbName = "couch_sink"
        self.testInit.setupCouch(dbName)
        
        # add corresponding part of the configuration for CouchSink(s)
        config = self.config.AlertProcessor
        config.critical.sinks.section_("couch")
        config.critical.sinks.couch.url = self.testInit.couchUrl
        config.critical.sinks.couch.database = self.testInit.couchDbName

        # just send the Alert into couch

        processor = Processor(config)
        rec = Receiver(self.addr, processor, self.ctrl)
        rec.startReceiver() # non blocking call
        
        # run worker(), this time directly without Process as above,
        # worker will send 10 Alerts to Receiver
        worker(self.addr, self.ctrl, 10)
        
        # wait until the Receiver is shut by worker
        while rec.isReady():
            time.sleep(0.1)
            
            
if __name__ == "__main__":
    unittest.main()