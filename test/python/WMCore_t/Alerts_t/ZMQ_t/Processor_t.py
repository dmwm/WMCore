#!/usr/bin/env python
# encoding: utf-8
"""
Processor_t.py

Created by Dave Evans on 2011-03-15.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import unittest
import time
from multiprocessing import Process, Queue
from WMCore.Alerts.ZMQ.Processor import Processor
from WMCore.Alerts.ZMQ.Sender    import Sender
from WMCore.Alerts.ZMQ.Receiver  import Receiver
from WMCore.Configuration import Configuration

def simpleWorker(addr, ctrl):
    """
    _simpleWorker_
    
    Sender that pauses and sends a shutdown message
    """
    time.sleep(1)
    s = Sender(addr, "Processor_t", ctrl )
    s.register()
    time.sleep(1)
    s.unregister()
    time.sleep(1)
    s.send_shutdown()
    return
    
def worker(addr, ctrl, nAlerts, workerId = "Processor_t"):
    """
    _worker_
    
    Send a few alerts 
    """
    time.sleep(1)
    s = Sender(addr, workerId, ctrl )
    s.register()
    time.sleep(1)
    for i in range(0, nAlerts):
        s({"Alert" : "Test", "Level" : i+5 })
        time.sleep(1)
    s.unregister()
    time.sleep(1)
    s.send_shutdown()
    
class Processor_t(unittest.TestCase):
    """
    TestCase for Processor.
    """
    def setUp(self):
        """
        set up for tests
        """
        self.addr = "tcp://127.0.0.1:5557"
        self.ctrl = "tcp://127.0.0.1:5559"
        
        self.config = Configuration()
        self.config.component_("AlertProcessor")
        self.config.AlertProcessor.section_("critical")
        self.config.AlertProcessor.section_("all")
        
        self.config.AlertProcessor.critical.level = 5
        self.config.AlertProcessor.all.level = 0
        self.config.AlertProcessor.all.buffer_size = 3
        
        
        self.config.AlertProcessor.critical.section_("sinks")
        self.config.AlertProcessor.all.section_("sinks")
        
        #self.config.AlertProcessor.critical.sinks.section_("email")
        #self.config.AlertProcessor.critical.sinks.section_("couch")
        #self.config.AlertProcessor.critical.sinks.section_("propagate")
        self.config.AlertProcessor.critical.sinks.section_("file")
        self.config.AlertProcessor.critical.sinks.file.outputfile = "/tmp/critical-alerts.json"
        
        self.config.AlertProcessor.all.sinks.section_("file")
        self.config.AlertProcessor.all.sinks.file.outputfile = "/tmp/all-alerts.json"
        
        #self.config.AlertProcessor.critical.sinks.couch.url = None
        #self.config.AlertProcessor.critical.sinks.couch.database = None
        
        #self.config.AlertProcessor.critical.sinks.email.fromAddr = "sfoulkes@fnal.gov"
        #self.config.AlertProcessor.critical.sinks.email.toAddr = ["sfoulkes@fnal.gov", "mnorman@fnal.gov", "meloam@fnal.gov"]
        #self.config.AlertProcessor.critical.sinks.email.smtpServer = "smtp.fnal.gov"
        
        
        
    # def testA(self):
    #     """test startup and shutdown of processor in receiver"""
    #     self.p = Process(target=simpleWorker, args=(self.addr, self.ctrl))
    #     self.p.start()
    #     
    #     rec = Receiver(self.addr, Processor(self.config.AlertProcessor))
    #     rec.start()
    # 
    #def testB(self):
    #    """
    #    test processing some alerts
    #    ToDo: work out some way of retrieving the alerts and checking them
    #    """
    #    self.p = Process(target=worker, args=(self.addr, self.ctrl, 10))
    #    self.p.start()
    #
    #    rec = Receiver(self.addr, Processor(self.config.AlertProcessor))
    #    rec.start()
        
    def testC(self):
        """
        test configuring sinks
        """
        #print str(self.config.AlertProcessor)
        p = Processor(self.config.AlertProcessor)
    
if __name__ == '__main__':
    unittest.main()