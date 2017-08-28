"""
File       : UnifiedInjectorManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: UnifiedInjectorManager class provides full functionality of the UnifiedInjector service.
"""

# futures
from __future__ import print_function, division

# system modules
import time

# Unified modules
from WMCore.Services.MicroService.Unified.Injector import injector
from WMCore.Services.MicroService.Unified.Common import reqmgrUrl, Options
from Utils.ProcessStats import processStatus, threadStack


class UnifiedInjectorManager(object):
    """
    Initialize UnifiedInjectorManager class
    """
    def __init__(self, config=None):
        self.config = config
        self.time0 = time.time()
        self.ncalls = 0

    def status(self):
        "Return current status about UnifiedInjector"
        sdict = {'server': processStatus()}
        sdict['server'].update({'uptime': time.time()-self.time0,
                                'ncalls': self.ncalls})
        sdict.update(threadStack())
        return sdict

    def request(self, **kwargs):
        "Process request given to UnifiedInjector"
        self.ncalls += 1
        url = kwargs.get('url', reqmgrUrl)
        spec = kwargs.get('spec', None)
        options = Options(kwargs)
        status = injector(url, options, spec)
        return {'status': status}
