#!/usr/bin/env python
"""
_MsgService_

This module implements the message service interface 
for inter-component communication . The message service follows the standard 
well known message passing approach, with the main objective being the reliable
delivery of messages between components. The message service is defined to
provide asynchronous delivery of messages, persistence and transaction
support.

The interface needs to be implemented supporting the appropriate query.
For example MySQL, Oracle, or another technology such as Twisted.

"""

__revision__ = \
    "$Id: MsgService.py,v 1.1 2008/08/26 13:55:56 fvlingen Exp $"
__version__ = \
    "$Revision: 1.1 $"
__author__ = \
    "fvlingen@caltech.edu"

import os
import socket
import logging
import threading

##############################################################################
# Message Service class
##############################################################################

class MsgService:
    """
    _MsgService_

    A message in the context of the message service is a structured data object
    that consists of a type and a payload. Components register in the message
    service by providing a string to be used as a component identifier. Through
    a subscription process, the components express their interest in getting
    messages of specific types. Components send messages (publish operation) by
    specifying both the type and the payload. Every time a component asks for a
    message (get operation) the message service returns the oldest not yet
    delivered message of any of the subscribed types.

    """
    
    ##########################################################################
    # Message Server class initialization
    ##########################################################################

    def __init__(self):
        """
        __init__
        
        """
        myThread = threading.currentThread()
        self.query = myThread.factory['msgService'].loadObject("Queries")
        self.name = "Assigne me a name"
        self.procid = 0

    ##########################################################################
    # register method 
    ##########################################################################

    def registerAs(self, name):
        """
        __registerAs__
        
        The operation registerAs registers the component as 'name' in the
        message service, including information on the host name and its PID.
        It is assumed that only one process with the same name is running in
        the production agent. Attempt to register a process with the same
        name will result in an update of its hostname and PID, since it is
        assumed that the old process crashed and it was started again.
        """
        # logging
        logging.info("Try registerAs "+str(name))
        # set component name
        self.name = name
        # get process data
        currentPid = os.getpid()
        currentHost = socket.gethostname()
      
        # check if process name is in database
        result = self.query.checkName(args = {'name' : name})
        # process was registered before
        if len(result) == 1:
            result = result[0]
            # if pid and host are the same, get id and return
            if result['host'] == currentHost and result['pid'] == currentPid:
                self.procid = result['procid']
                return
            # process was replaced, update info
            else:
                self.query.updateName(args = {'currentHost' : currentHost, \
                    'currentPid' : currentPid, 'name' : name})
                self.procid = result['procid']
                return
        # register new process in database
        logging.debug("Registering new process with name: "+ name)
        self.query.insertProcess(args = {'host' : currentHost, \
            'pid' : currentPid, 'name' : name})
        # get id
        self.dbid = self.query.lastInsertId()
        #self.procid =
        
        
      
        
    ##########################################################################
    # subscribe method
    ##########################################################################

    def subscribeTo(self, name ):
        """
        __subscribeTo__
        
        The operation subscribeTo subscribes the current component to messages
        of type 'name'. 
        
        The message type is registered in the database if it was not
        registered before.
        """
        pass
 
    ##########################################################################
    # priority subscribe method
    ##########################################################################

    def prioritySubscribeTo(self, name):
        """
        __prioritySubscribeTo__
        
        The operation prioritySubscribeTo subscribes the current component to messages
        of type 'name'. 
        
        The message type is registered in the database if it was not
        registered before.
        """
        pass
      
    ##########################################################################
    # publish method 
    ##########################################################################

    def publish(self, args):
        """
        _publish_
        
        The operation publish sends the message of type 'name' with content
        specified by 'payload' to all components subscribed to this message
        type.

        The message type is registered in the database if it was not
        registered before.

        args is a dictionary of name,payload, and delay (latter is optional)
        or an array of dictionaryies of name,payload and delay. All items are strings.
        e.g. no delay is: '00:00:00' which is the default.

        Returns the number of destinations to where the message will be delivered.
        
        publication of the message can be instant (default) (by adding the instant: True
        to the dictionary, or delayed (waits until the finish method is called.
        
         
        {'name' : 'myMessage', 'payload' : 'myPayload', 'delay' : '10:30:45', 'instant' : False}
        """
        pass
    
    def publishUnique(self, args):
        """
        __publishUnique__
        
        publish method that only publishes if no
        messages of the same type type exist. 
        
        Returns a number of destinations where the message will be delivered.
        
        publication of the message can be instant (default) (by adding the instant: True
        to the dictionary, or delayed (waits until the finish method is called by adding
        instant : False.
        
        {'name' : 'myMessage', 'payload' : 'myPayload', 'delay' : '10:30:45', 'instant' : False}
        """
        pass
              
    ##########################################################################
    # get method 
    ##########################################################################

    def get(self, wait = True):
        """
        __get__
        
        The operations get returns both the type and the payload of a single
        message.
        
        Polling is performed in this prototype implementation to wait for new
        messages.
        """
        pass
    
    ##########################################################################
    # finish method 
    ##########################################################################

    def finish(self):
        """
        __finish__
        
        called after the messages has been handled. this is to prevent long open
        connections underlying databases. 
        
        MsgService.get()
        ...<do your potentially long standing operations>...
        MsgService.finish()
        """
        pass
    ##########################################################################
    # purgeMessages method 
    ##########################################################################

    def purgeMessages(self):
        """
        __purgeMessages__
        
        Drop all messages to be delivered. 
        """

    ##########################################################################
    # remove messages of a certain time addressed to me
    ##########################################################################

    def remove(self, messageType):
        """
        __remove__

        Remove all messages of a certain type addressed to me.
        """
        pass

    ##########################################################################
    # remove messages in history
    ##########################################################################

    def cleanHistory(self, hours):
        """
        __cleanHistory__
        
        Delete history messages older than the number of hours
        specified.
        
        Performs an implicit commit operation.
        
        Arguments:
        
            hours -- the number of hours.
        
        """
        pass
      
