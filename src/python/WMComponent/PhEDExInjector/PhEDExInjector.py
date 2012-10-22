#!/usr/bin/env python
"""
_PhEDExInjector_

Poll DBSBuffer and inject files into PhEDEx after they've been injected into
DBS.
"""

import threading
import logging

from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory

from WMComponent.PhEDExInjector.PhEDExInjectorPoller import PhEDExInjectorPoller
from WMComponent.PhEDExInjector.PhEDExInjectorSubscriber import PhEDExInjectorSubscriber

class PhEDExInjector(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)

    def preInitialization(self):
        pollInterval = self.config.PhEDExInjector.pollInterval
        subInterval = self.config.PhEDExInjector.subscribeInterval
        logging.info("Setting poll interval to %s seconds for inject" % pollInterval)


        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(PhEDExInjectorPoller(self.config), pollInterval)

        if getattr(self.config.PhEDExInjector, "subscribeDatasets", False):
            logging.info("Setting poll interval to %s seconds for subscribe" % subInterval)
            myThread.workerThreadManager.addWorker(PhEDExInjectorSubscriber(self.config), subInterval)
