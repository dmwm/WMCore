#!/usr/bin/env python
"""
_Dummy_

A dummy class to mimic the component for tets.
"""

import logging
import os
import threading

class DBCoreDummy:
    def __init__(self):
        self.connectUrl = os.getenv("DATABASE")

class ConfigDummy:
    def __init__(self):
        self.CoreDatabase = DBCoreDummy()

class Dummy():
    """
    _Dummy_

    A dummy class to mimic the component for tests.
    """

    def __init__(self):
        logging.debug("I am a dummy object!")
        self.config = ConfigDummy()

    def prepareToStop(self):
        """Copied from Agent.Harness"""
        # Stop all worker threads
        logging.info(">>>Terminating worker threads")
        myThread = threading.currentThread()
        myThread.workerThreadManager.terminateWorkers()
