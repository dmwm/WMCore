#!/usr/bin/env python
# pylint: disable=W0613

"""
_RetryManager_

The retry manager picks up jobs from their cooloff state and using a set of plugins
attempts to put them in their non-cooloff state again.

"""

import logging
import threading

from WMCore.Agent.Harness import Harness
from WMComponent.RetryManager.RetryManagerPoller import RetryManagerPoller


class RetryManager(Harness):
    """
    _RetryManager_

    The retry manager picks up jobs from their cooloff state and using a set
    of plugins attempts to put them in their non-cooloff state again
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.config = config

    def preInitialization(self):
        """
        Initializes plugins for different messages
        """

        # Add event loop to worker manager
        myThread = threading.currentThread()
        pollInterval = self.config.RetryManager.pollInterval
        logging.info("Setting poll interval to %s seconds", pollInterval)
        myThread.workerThreadManager.addWorker(RetryManagerPoller(self.config), pollInterval)
