#!/usr/bin/env python
"""
Handler for PhEDExInjection requets
"""
__all__ = []
__revision__ = "$Id: NewInjectionHandler.py,v 1.1 2009/08/11 21:36:56 meloam Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

class NewInjectionHandler(BaseHandler):
    def __init__(self, component):
        BaseHandler.__init__(self, component)
        print "MAKESTART"

     # this we overload from the base handler
    def __call__(self, event, payload):
        print "STARTCALL"
        self.component.inject(event)


