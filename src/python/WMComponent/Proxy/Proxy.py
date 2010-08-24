#!/usr/bin/env python

"""
_Proxy_

The proxy component relays messages to other prodagent instances.
Initially it is designed to communicate with the older prodagent components.
"""






import cPickle
import threading

# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness

from WMComponent.Proxy.Handler.CheckProxy import CheckProxy
from WMComponent.Proxy.Handler.Dispatcher import Dispatcher
from WMComponent.Proxy.ProxyQueue import ProxyQueue

class Proxy(Harness):
    """
    _Proxy_
    
    The proxy component relays messages to other prodagent instances.
    Initially it is designed to communicate with the older prodagent components.
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)

    def preInitialization(self):
        """
        Reads the config file to filter out proxy parameters
        and setup channels/queues to the remote proxy/components.
        """
        self.addMsgLock = threading.Lock()
        # a handler we use to dispatch messages from our message
        # service to proxies that have subscribed to them.
        self.dispatcher = Dispatcher(self)

        # map the check proxy message to the periodic check proxy handler.
        self.messages['CheckProxy'] = CheckProxy(self)
        # get a dictionary of the config section
        # to filter out all proxy addresses defined in our config file.
        self.proxies = {}
        sectionDict = self.config.Proxy.dictionary_()
        for proxy in sectionDict.keys():
            # we prefix proxies parameters with PXY_
            if proxy.rfind("PXY_") == 0:
                # pickle is used to store more complex parameters.
                details = \
                    cPickle.loads(self.config.Proxy.__getattribute__(proxy))
                self.proxies[proxy] = details
                # associate proxy addresses with a queu in and out for 
                # sending and receiving messages.
                self.proxies[proxy]['queueOut'] = ProxyQueue(proxy, details) 
                self.proxies[proxy]['queueIn'] = ProxyQueue(proxy, details) 
        #FIXME: check if we had previous subscriptions stored in the database.

    def addMessage(self, proxy, msgType):
        """
        Adds a message type to the self.message handler mapping
        This needs to be done in a thread safe manner as  the call for this
        method comes from the threads in the CheckProxy threadpool.
        """
        self.addMsgLock.acquire()
        # associate the message type to our dispatcher.
        self.messages[msgType] = self.dispatcher
        # register the proxy and msgType in our dispatcher
        # as the message service only registers the type (we need to proxy too)
        self.dispatcher.subscribeTo(msgType, proxy)

        self.addMsgLock.release()

    def postInitialization(self):
        """
        Message is overloaded to remove any check proxy messages
        and publish a new one.
        """
        myThread = threading.currentThread()
        # first remove all check proxy messages
        myThread.msgService.remove("CheckProxy")
        # then publish a check proxy for all the proxies in 
        # the config file.
        msgs = []
        # publish periodic check proxy message
        for proxy in self.proxies.keys():
            msg = {'name':'CheckProxy', 'payload':proxy, \
                'delay':self.config.Proxy.contactIn}
            msgs.append(msg)
            # publish subscription requests for the individual proxies
            for subscription in self.proxies[proxy]['subscription']:
                msg = {'name':'ProxySubscribe', 'payload':subscription}
                self.proxies[proxy]['queueOut'].insert(msg)
            # also subscribe to the ProxySubscribe 
            # from the proxy we connected to.
            msg = {'name':'ProxySubscribe', 'payload':'ProxySubscribe'}
            self.proxies[proxy]['queueOut'].insert(msg)
                    
        
        # publish the periodic messages for checking the proxy channels
        myThread.msgService.publish(msgs) 
      
