#!/usr/bin/env python
"""
_PromptSkimScheduler_

PromptSkimScheduler harness.  Instantiate the PromptSkimPoller and have it poll
T0AST for transfered blocks.
"""

__revision__ = "$Id: PromptSkimScheduler.py,v 1.1 2010/06/04 16:24:48 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Agent.Harness import Harness
from WMComponent.PromptSkimScheduler.PromptSkimPoller import PromptSkimPoller

class PromptSkimScheduler(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)
        return

    def preInitialization(self):
        pollInterval = self.config.PromptSkimScheduler.pollInterval
        myThread = threading.currentThread()        
        myThread.workerThreadManager.addWorker(PromptSkimPoller(self.config), pollInterval)
