#!/usr/bin/env python
"""
Handler for checking messages in a proxy.
"""
__all__ = []
__revision__ = "$Id: CheckProxy.py,v 1.1 2008/09/19 15:34:34 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

class CheckProxy(BaseHandler):
    """
    Handler for checking messages in a proxy.
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # define a slave threadpool. The number of threads
        # is the number of proxies we contact.

        sectionDict = self.component.config.Proxy.dictionary_()
        proxies = 0
        for key in sectionDict.keys():
            if key.rfind("PXY_") == 0:
                proxies += 1
        
        self.threadpool = ThreadPool(\
            "WMComponent.Proxy.Handler.CheckProxySlave", \
            self.component, 'CheckProxy', proxies) 

     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # as we defined a threadpool we can enqueue our item
        # and move to the next.
        self.threadpool.enqueue(event, payload)


