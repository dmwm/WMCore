#!/usr/bin/env python
"""
_AlertProcessor_

Component that runs an alert Processor pipeline to forward alerts to various other systems & monitoring

"""

import logging
from multiprocessing import Process

from WMCore.Agent.Harness import Harness
from WMCore.Alerts.ZMQ.Processor import Processor
from WMCore.Alerts.ZMQ.Receiver  import Receiver
from WMCore.Alerts.ZMQ.Sender  import Sender


def startProcessor(listensOn, controlOn, config):
    """
    _startProcessor_
    
    Start the ZMQ alert processor
    """
    rec = Receiver(listensOn, Processor(), controlOn)
    rec.start()
    


    
    


class AlertProcessor(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)
        self.config = config
        self.process = None
        
    def preInitialization(self):
        """
        _preInitialization_
        
        Start up the zmq Processor
        """
        confSect = self.config.AlertProcessor
        target  = confSect.processorListensOn 
        control = confSect.processorControlOn 
        
        self.process = Process(target = startProcessor, args = (target, control, confSect))
        self.process.start()
        
        
        
    def stopProcessor(self):
        """
        _stopProcessor_
        
        Method to shutdown the Alert Processor
        """
        s = Sender(listensOn, "AlertProcessor.stopProcessor", controlOn)
        s.send_shutdown()
        self.process.terminate()
        
        
    def prepareToStop(self, wait = False, stopPayload = ""):
        """
        _prepareToStop_
        
        Override prepareToStop to include call to stopProcessor
        Fugly, but seems no other way to do this...
        """
        self.stopProcessor()
        Harness.prepareToStop(self, wait, stopPayload)
        
        
   
  
     