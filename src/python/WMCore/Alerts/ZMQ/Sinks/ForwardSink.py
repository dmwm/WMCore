"""
ForwardSink - send / forward alerts to another Receiver.

"""

import logging

from WMCore.Alerts.ZMQ.Sender import Sender



class ForwardSink(object):
    """
    Alert forwarder to another alert processor, resp. to some Receiver which
    owns AlertProcessor.
    
    """        
    def __init__(self, config):
        self.config = config
        self.address = config.address
        self.label = getattr(config, "label", None)
        self.controlAddr = getattr(config, "controlAddr", None)
        self.sender = Sender(self.address, label = self.label,
                             controller = self.controlAddr)
        logging.debug("%s initialized." % self.__class__.__name__)
        
    
    def send(self, alerts):
        """
        Handle list of alerts.
        
        """
        [self.sender(a) for a in alerts]
        m = "%s sent alerts." % self.__class__.__name__
        logging.debug(m)