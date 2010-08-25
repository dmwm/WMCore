#!/usr/bin/env python
"""
_PromptSkimPoller_

Poll T0AST for complete blocks and launch skims.
"""

__revision__ = "$Id: PromptSkimPoller.py,v 1.1 2010/06/04 16:24:48 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import time
import threading
import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Agent.Harness import Harness
from WMCore.DAOFactory import DAOFactory

class PromptSkimPoller(BaseWorkerThread):
    def __init__(self, config):
        BaseWorkerThread.__init__(self)
        self.config = config
        return
    
    def setup(self, parameters = None):
        """
        _setup_

        """
        return
    
    def algorithm(self, parameters = None):
        """
        _algorithm_

        Poll T0AST for completed blocks.
        """
        return
