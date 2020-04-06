#!/usr/bin/env python
"""
_RucioInjector_

Poll the DBSBuffer tables and insert files, blocks and datasets into Rucio
"""

import threading
import logging

from WMCore.Agent.Harness import Harness

from WMComponent.RucioInjector.RucioInjectorPoller import RucioInjectorPoller

class RucioInjector(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)

    def preInitialization(self):
        pollInterval = self.config.RucioInjector.pollInterval
        logging.info("Setting poll interval to %s seconds for inject", pollInterval)

        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(RucioInjectorPoller(self.config), pollInterval)
