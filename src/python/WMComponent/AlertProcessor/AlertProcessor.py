"""
Component of WMAgent that runs an alert Processor pipeline to forward
alerts to various other systems & monitoring.

"""

import logging

from WMCore.Agent.Harness import Harness
from WMCore.Alerts.ZMQ.Processor import Processor
from WMCore.Alerts.ZMQ.Receiver import Receiver
    


class AlertProcessor(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)
        self._myName = self.__class__.__name__
        self.config = config
        # instance of processor
        self._processor = None
        # instance of Receiver which owns Processor (self._processor) 
        # and runs on background
        self._receiver = None
        logging.info("%s initialized." % self._myName)
        
        
    def preInitialization(self):
        """
        Start up the ZMQ Receiver + Processor.
        
        """
        logging.info("%s starting ..." % self._myName)
        self._processor = Processor(self.config.AlertProcessor)
        # Receiver listens on work channel (address) and on control
        # channel (controlAddr)
        self._receiver = Receiver(self.config.AlertProcessor.address,
                                  self._processor,
                                  self.config.AlertProcessor.controlAddr)
        self._receiver.startReceiver()
        logging.info("%s started, Receiver should be listening." % self._myName)
        
        
    def stopProcessor(self):
        """
        Method to shutdown the Alert Processor.
        
        """
        logging.info("%s shutting down, waiting for Receiver ..." % self._myName)
        self._receiver.shutdown()
        
        
    def prepareToStop(self, wait = False, stopPayload = ""):
        """
        Override prepareToStop to include call to stopProcessor.
        Ugly, but seems no other way to do this...
        
        """
        self.stopProcessor()
        Harness.prepareToStop(self, wait, stopPayload)