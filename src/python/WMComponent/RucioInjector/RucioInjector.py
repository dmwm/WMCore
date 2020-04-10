#!/usr/bin/env python
"""
_RucioInjector_

Poll the DBSBuffer tables and insert files, blocks and datasets into Rucio
"""
from __future__ import division

import logging
import threading

from WMComponent.RucioInjector.RucioInjectorPoller import RucioInjectorPoller
from WMCore.Agent.Harness import Harness


class RucioInjector(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)

    def preInitialization(self):
        pollInterval = self.config.RucioInjector.pollInterval
        logging.info("Setting poll interval to %s seconds for inject", pollInterval)

        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(RucioInjectorPoller(self.config), pollInterval)
