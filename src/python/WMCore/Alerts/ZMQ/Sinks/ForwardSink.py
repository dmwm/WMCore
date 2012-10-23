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
        logging.info("Instantiating ...")
        self.label = getattr(config, "label", None)
        self.controlAddr = getattr(config, "controlAddr", None)
        self.sender = Sender(self.address, controller = self.controlAddr,
                             label = self.label)
        logging.info("Initialized.")


    def send(self, alerts):
        """
        Handle list of alerts.

        """
        [self.sender(a) for a in alerts]
        logging.debug("Sent %s alerts." % len(alerts))
