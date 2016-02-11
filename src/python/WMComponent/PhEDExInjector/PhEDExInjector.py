#!/usr/bin/env python
"""
_PhEDExInjector_

Poll DBSBuffer and inject files into PhEDEx after they've been injected into
DBS.
"""

import threading
import logging

from WMCore.Agent.Harness import Harness

from WMComponent.PhEDExInjector.PhEDExInjectorPoller import PhEDExInjectorPoller

class PhEDExInjector(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)

    def preInitialization(self):
        pollInterval = self.config.PhEDExInjector.pollInterval
        logging.info("Setting poll interval to %s seconds for inject", pollInterval)

        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(PhEDExInjectorPoller(self.config), pollInterval)
