#!/usr/bin/env python
"""
_PhEDExInjector_

Poll DBSBuffer and inject files into PhEDEx after they've been injected into
DBS.
"""

import threading
import logging
import time

from WMCore.Agent.Harness import Harness

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

from WMComponent.PhEDExInjector.PhEDExInjectorPoller import PhEDExInjectorPoller
from WMComponent.PhEDExInjector.PhEDExInjectorSubscriber import PhEDExInjectorSubscriber

class PhEDExInjector(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)

    def preInitialization(self):
        pollInterval = self.config.PhEDExInjector.pollInterval
        subInterval = self.config.PhEDExInjector.subscribeInterval
        logging.info("Setting poll interval to %s seconds for inject", pollInterval)

        # retrieving the node mappings is fickle and can fail quite often
        # hence only do it once (with retries) and pass it to the workers
        phedex = PhEDEx({"endpoint": self.config.PhEDExInjector.phedexurl}, "json")
        try:
            nodeMappings = phedex.getNodeMap()
        except Exception:
            time.sleep(2)
            try:
                nodeMappings = phedex.getNodeMap()
            except Exception:
                time.sleep(4)
                nodeMappings = phedex.getNodeMap()

        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(PhEDExInjectorPoller(self.config, phedex, nodeMappings), pollInterval)

        if getattr(self.config.PhEDExInjector, "subscribeDatasets", False):
            # wait a bit for first poll cycle of PhEDExInjectorPoller to complete
            # hopefully avoids intermingled logs (which can be confusing)
            time.sleep(2)
            logging.info("Setting poll interval to %s seconds for subscribe", subInterval)
            myThread.workerThreadManager.addWorker(PhEDExInjectorSubscriber(self.config, phedex, nodeMappings), subInterval)
