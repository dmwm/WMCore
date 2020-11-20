"""
File       : UnifiedInjectorManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: UnifiedInjectorManager class provides full functionality of the UnifiedInjector service.
"""

# futures
from __future__ import division

# system modules
import time

# Unified modules
from Utils.ProcessStats import processStatus, threadStack


class UnifiedInjectorManager(object):
    """
    Initialize UnifiedInjectorManager class
    """
    def __init__(self, config=None):
        self.config = config
        self.time0 = time.time()
        self.state = None
        self.ncalls = 0

    def status(self):
        "Return current status about UnifiedInjector"
        sdict = {'server': processStatus()}
        sdict['server'].update({'uptime': time.time()-self.time0,\
                'ncalls': self.ncalls, 'state': self.state})
        sdict.update(threadStack())
        return sdict

    def request(self, **kwargs):
        "Process request given to UnifiedInjector"
        return {'state': self.state}
