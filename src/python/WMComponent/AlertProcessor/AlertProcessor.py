"""
Component of WMAgent that runs an alert Processor pipeline to forward
alerts to various other systems & monitoring.

"""

import logging
import signal
import traceback

from WMCore.Agent.Harness import Harness
from WMCore.Alerts.ZMQ.Processor import Processor
from WMCore.Alerts.ZMQ.Receiver import Receiver



class AlertProcessor(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)
        self.config = config
        # instance of processor
        self._processor = None
        # instance of Receiver which owns Processor (self._processor)
        # and runs on background
        self._receiver = None
        #3602 related:
        # Harness, nor the components, handle signal.SIGTERM which
        # is used by wmcoreD --shutdown, hence shutdown sequence is not called
        # this shall later be moved into (hopefully largely improved) Harness
        signal.signal(signal.SIGTERM, self._signalHandler)


    def _signalHandler(self, signalNumber, frame):
        logging.info("Signal number %s caught." % signalNumber)
        self.prepareToStop()


    def preInitialization(self):
        """
        Start up the ZMQ Receiver + Processor.

        """
        logging.info("preInitialization ...")
        # something fishy (again) going on in Harness, wmcoreD
        # component may fail, still will be considered as running (#3602)
        # this is why #3320 is difficult to fix ... wmcoreD would happily
        # continue even after raising an exception even from this very method directly
        self._processor = Processor(self.config.AlertProcessor)
        # Receiver listens on work channel (address) and on control
        # channel (controlAddr)
        self._receiver = Receiver(self.config.AlertProcessor.address,
                                  self._processor,
                                  self.config.AlertProcessor.controlAddr)
        self._receiver.startReceiver()
        logging.info("preInitialization - finished.")


    def stopAlertProcessor(self):
        """
        Method to shutdown the AlertProcessor.

        """
        logging.info("stopAlertProcessor - stopping Receiver ...")
        self._receiver.shutdown()
        logging.info("stopAlertProcessor finished.")


    def prepareToStop(self, wait = False, stopPayload = ""):
        """
        Override prepareToStop to include call to stopProcessor.
        Ugly, but seems no other way to do this...

        """
        logging.info("Shutting down the component - prepareToStop ...")
        self.stopAlertProcessor()
        Harness.prepareToStop(self, wait, stopPayload)
        logging.info("prepareToStop finished.")
