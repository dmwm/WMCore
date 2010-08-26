#!/usr/bin/env python
"""
_CheckProxySlave_

Slave object for threadpool like (asynchronous)
checking of remote proxies.

"""
__all__ = []
__revision__ = "$Id: CheckProxySlave.py,v 1.2 2008/09/29 16:10:56 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "fvlingen@caltech.edu"



import threading

from WMCore.ThreadPool.ThreadSlave import ThreadSlave


class CheckProxySlave(ThreadSlave):
    """
    _CheckProxySlave_
    
    Slave object for threadpool like (asynchronous)
    checking of remote proxies.
    
    """

    def __call__(self, parameters):
        """
        Checks the message type and either publishes or
        if it is a meta message handles it.
        """

        # check what the type of message is
        # this does not have to be threadsafe as whe have
        # one checkproxy per proxy channel.
        # check if there are messages for us.
        type, payload = \
            self.component.proxies[parameters['payload']]['queueIn'].retrieve()
        myThread = threading.currentThread()
        while (type != None) and (payload !=None):
            # here we handle some special messages 
            if type == 'ProxySubscribe':
                myThread.transaction.begin()
                myThread.msgService.subscribeTo(payload)
                myThread.transaction.commit()
                # map a handler to this message type in a threadsafe way
                self.component.addMessage(parameters['payload'], payload)
            else:
                # message was not a subscription one, just publish it.
                myThread.transaction.begin()
                msg = {'name':type, 'payload':payload}
                myThread.msgService.publish(msg)
                myThread.transaction.commit()
            type, payload = self.component.\
                proxies[parameters['payload']]['queueIn'].retrieve()
            
        # we dealt wit all the messages, now throw a
        # periodic message again to check the queue.
        msg = {'name':'CheckProxy', 'payload':parameters['payload'], \
            'delay':self.component.config.Proxy.contactIn}
        myThread.transaction.begin()
        # publish has NOT! integrated begin/commit statements
        myThread.msgService.publish(msg)
        myThread.transaction.commit()
        # need to call the finish ourselves as we are in a thread.
        # finish has integrated begin/commit statements
        myThread.msgService.finish()


        
      
