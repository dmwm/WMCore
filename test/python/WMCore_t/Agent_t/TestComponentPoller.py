#!/usr/bin/env python

"""
_TestComponentPoller_
Poller to test the skeleton
"""
from WMCore.WorkerThreads.BaseWorkerThread    import BaseWorkerThread

class TestComponentPoller(BaseWorkerThread):
    """
    _TestComponentPoller_
    """
    def __init__(self, config):
        """
        __init__
        """
        BaseWorkerThread.__init__(self)

    def algorithm(self, parameters = None):
        """
        The poller algorithm is written here
        """
        return
