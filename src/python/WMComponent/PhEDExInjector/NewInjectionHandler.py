#!/usr/bin/env python
"""
Handler for PhEDExInjection requets
"""
__all__ = []




from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

class NewInjectionHandler(BaseHandler):
    def __init__(self, component):
        BaseHandler.__init__(self, component)

     # this we overload from the base handler
    def __call__(self, event, payload):
        self.component.inject(event)


