"""
ZMQ Sender, Alerts framework.

"""


import os
import logging

import zmq

from WMCore.Alerts.Alert import RegisterMsg, UnregisterMsg, ShutdownMsg



class Sender(object):
    """
    ZMQ sender to dispatch alerts to a target.

    """
    # this delay specifies how long to wait when there are un-delivered
    # messages in the ZMQ buffer when closing the socket (channel) / context.
    # some messages may be lost but solves the issue of hanging esp. in the
    # test when there is no receiver available (ticket #1837)
    LINGER_DELAY = 1000 # [ms]


    def __init__(self, target, controller, label = None):
        self._label = label or "Sender_%s" % os.getpid()
        self._context = zmq.Context()
        # set up a channel to send work
        self._workChannel = self._context.socket(zmq.PUSH)
        self._workChannel.setsockopt(zmq.LINGER, self.LINGER_DELAY)
        self._workChannel.connect(target)
        # set up a control channel
        self._contChannel = self._context.socket(zmq.PUB)
        self._contChannel.setsockopt(zmq.LINGER, self.LINGER_DELAY)
        self._contChannel.connect(controller)
        # socket closure will be done on garbage collection of Sender instance


    def __call__(self, alert):
        """
        Send the alert instance to the target that this sender represents.

        """
        self._workChannel.send_json(alert)
        logging.debug("Alert %s sent." % alert)


    def register(self):
        """
        Send a register message to the target.

        """
        self._contChannel.send_json(RegisterMsg(self._label))
        logging.debug("Register message sent for %s." % self._label)


    def unregister(self):
        """
        Send an unregister message to the target.

        """
        self._contChannel.send_json(UnregisterMsg(self._label))
        logging.debug("Unregister message sent for %s." % self._label)


    def sendShutdown(self):
        """
        Tells the Receiver to shut down.
        This method mostly here for convenience in tests.

        """
        self._contChannel.send_json(ShutdownMsg())
        logging.debug("Shutdown message sent.")
