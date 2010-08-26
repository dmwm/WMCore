#!/usr/bin/env python
"""
Handler for PhEDExInjection requets
"""
__all__ = []
__revision__ = "$Id: NewInjectionHandler.py,v 1.2 2009/08/24 11:10:03 meloam Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

class NewInjectionHandler(BaseHandler):
    def __init__(self, component):
        BaseHandler.__init__(self, component)

     # this we overload from the base handler
    def __call__(self, event, payload):
        self.component.inject(event)


