"""
File       : UnifiedTransferorManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: UnifiedTransferorManager class provides full functionality of the UnifiedTransferor service.
"""

# futures
from __future__ import print_function, division

# system modules

# WMCore modules
from WMCore.Services.MicroService.Unified.RequestInfo import requestsInfo

class UnifiedTransferorManager(object):
    """
    Initialize UnifiedTransferorManager class
    """
    def __init__(self, config=None):
        self.config = config
        self.requests = {}

    def status(self):
        "Return current status about UnifiedTransferor"
        sdict = {}
        return sdict

    def request(self, **kwargs):
        "Process request given to UnifiedTransferor"
        return {}

    def worker(self, state='assignment-approved'):
        "Worked for this manager class"
        self.requests = requestsInfo(state)
