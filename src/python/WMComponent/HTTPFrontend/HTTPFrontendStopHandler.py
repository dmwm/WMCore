#!/usr/bin/env python
"""
Handler for cherrypy stop signals
"""
__all__ = []




from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

class HTTPFrontendStopHandler(BaseHandler):
    def __init__(self, component):
        BaseHandler.__init__(self, component)

     # this we overload from the base handler
    def __call__(self, event, payload):
        self.component.stop()


