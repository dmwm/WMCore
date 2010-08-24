#!/usr/bin/env python
"""
Handler for dispatching messages to other proxies.
"""
__all__ = []
__revision__ = "$Id: Dispatcher.py,v 1.2 2008/09/29 16:10:56 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "fvlingen@caltech.edu"


from WMCore.Agent.BaseHandler import BaseHandler

class Dispatcher(BaseHandler):
    """
    Handler for dispatching messages to other proxies.
    This object does not have to be thread safe as it is either
    called in a threadsafe method, or it is invoked (in sequence)
    by messages from the message queue.
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # define a slave threadpool. The number of threads
        # is the number of proxies we contact.

        # prepare the msgType to proxy mapping
        self.msgType2Proxy = {}

    def subscribeTo(self, msgType, proxy):
        """
        Stores the mapping from msgType to  the remote proxies.
        """
        # FIXME: these needs to be made persitent in case of crash.
        if not self.msgType2Proxy.has_key(msgType):
            self.msgType2Proxy[msgType] = {}
        if not self.msgType2Proxy[msgType].has_key(proxy):
            self.msgType2Proxy[msgType][proxy] = 'sendIt2Me'

    # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload. It needs to put in all
        the queues of proxies that want this message.
        """
        msg = {'name':event, 'payload':payload['payload']}
        if self.msgType2Proxy.has_key(event):
            for proxy in self.msgType2Proxy[event].keys():
                if self.component.proxies.has_key(proxy):
                    # this does not have to be thread safe as the threads
                    # only check the queue in.
                    self.component.proxies[proxy]['queueOut'].insert(msg)

