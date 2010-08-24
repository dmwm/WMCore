#!/usr/bin/env python
#pylint: disable-msg=E1103
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

This implementation for mysql contains a few optimization w.r.t.
the previous implementation:

-use of buffers for message queue to prevent single row
inserts in large tables
-table that specifies if there is a message in the queue for a component
to prevent unecessary queries on a large message queue
-mutli queue option (can be turned of/on): each component creates his own 
tables, based on the name it registers with and messages for this 
component are stored in these tables, thereby keeping the queue 
length managable. This option creates 6 tables per component 
(e.g. 10 components create 60 tables)
-priority messages: separate tables whos messages are examined before
the normal messages.
-periodic purging of history queue with option to keep always the last
<x> messages in the history queue.
-no long open commits. service contains a finish method that removes
messages after it is handled.
"""

__revision__ = \
    "$Id: MsgService.py,v 1.6 2008/09/09 13:50:35 fvlingen Exp $"
__version__ = \
    "$Revision: 1.6 $"
__author__ = \
    "fvlingen@caltech.edu"

import os
import socket
import logging
import threading
import time

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION

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
        # several attributes we use for holding messages 
        # until they can be delivered (for the oneQueue model)
        self.priorityMsg = {}
        self.msg = {}
        self.instantMsg = {}
        self.instantPriorityMsg = {}
        # buffer sizes 
        self.bufferSize = 400
        # history_dump size
        self.historySize = 1000
        # minimum amount of history we want to keep
        self.historyMin = 100
        # this means we can have one queue (with buffers)
        # or separate queues (one per component). The latter
        # will lead to more tables (6 per component)
        self.oneQueue = True
        # what is the message currently being worked on.
        self.currentMsg = None
        # from what table did we get the message (important for finish action)
        self.currentMsgTable = None
        # how long do we wait to check again for messages.
        self.pollTime = 5

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
        # check if component name does not contain a reserved word:
        reserved = ['ms_message', 'ms_priority_message', \
                    'buffer_out', 'buffer_in']
        for word in reserved:
            if name.find(word) >=0:
                raise WMException(WMEXCEPTION['WMCORE-7']+ \
                    ">>"+word+"<<",'WMCORE-7')
        # set component name
        self.name = name
        # get process data
        currentPid = os.getpid()
        currentHost = socket.gethostname()
      
        # check if process name is in database
        result = self.query.checkName(args = {'name' : name})
        # process was registered before
        if result != {}:
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
        self.procid = self.query.lastInsertId()
        # update message arrived tables
        self.query.initializeAvailable(args = {'procid' : self.procid})

        if not self.oneQueue:
            msg = "Create additional tables for this component: multiQueue"
            logging.debug(msg)
            self.query.insertComponentMsgTables(name) 
        
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
        # logging
        logging.debug("subscribeTo requested")
        # check if message type is in database
        result = self.query.checkMessageType(args = {'name' : name})
        if result != {}:
            # message type was registered before, get id
            typeid = result['typeid']
        else:
            # not registered before, so register now
            self.query.insertMessageType({'name' : name})
            # get id
            typeid = self.query.lastInsertId()
        # check if there is an entry in subscription table
        result = self.query.checkSubscription(args = {'procid' : self.procid, \
            'typeid' : typeid})
        # entry registered before, just return
        if result != {}:
            return
        # not registered, do it now
        self.query.insertSubscription(args = {'procid' : self.procid, \
            'typeid' : typeid})

 
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
        # logging
        logging.debug("prioritySubscribeTo requested")
        # check if message type is in database
        result = self.query.checkMessageType(args = {'name' : name})
        if result != {}:
            # message type was registered before, get id
            typeid = result['typeid']
        else:
            # not registered before, so register now
            self.query.insertMessageType({'name' : name})
            # get id
            typeid = self.query.lastInsertId()
        # check if there is an entry in subscription table
        result = self.query.checkPrioritySubscription(args = \
            {'procid' : self.procid, 'typeid' : typeid})
        # entry registered before, just return
        if result != {}:
            return
        # not registered, do it now
        self.query.insertPrioritySubscription(args = {'procid' : self.procid, \
            'typeid' : typeid})
      
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

        publication of the message can be instant (default) (by adding the instant: True
        to the dictionary, or delayed (waits until the finish method is called.
        
         
        {'name' : 'myMessage', 'payload' : 'myPayload', 'delay' : '10:30:45', 'instant' : False}
        """
        
        # logging
        logging.debug("publish requested")
        if type(args) == dict:
            args = [args]
        # find message types first
        messageTypes = {}
        for message in args:
            messageTypes[message['name']] = {}

        for messageType in messageTypes.keys():
            # check if message type is in database
            result = self.query.checkMessageType(args = {'name' : messageType})
            # get message type id
            if result != {}:
                # message type was registered before, get id
                messageTypes[messageType]['typeid'] = result['typeid']
            else:
                self.query.insertMessageType({'name' : messageType})
                # get id
                typeid = self.query.lastInsertId()
                messageTypes[messageType]['typeid'] = typeid
            # get destinations
            messageTypes[messageType]['destinations'] = \
                self.query.getDestinations(args = \
                {'typeid' : messageTypes[messageType]['typeid']})
            messageTypes[messageType]['priorityDestinations'] = \
                self.query.getPriorityDestinations(args = \
                {'typeid' : messageTypes[messageType]['typeid']})

        # format messages
        for message in args:
            destinations = messageTypes[message['name']]['destinations']
            priorityDestinations = \
                messageTypes[message['name']]['priorityDestinations']
            typeid = messageTypes[message['name']]['typeid']
            payload = message['payload']
            if message.has_key('delay'):
                delay = args['delay']
            else:
                delay = "00:00:00"
            if not message.has_key('instant'):
                message['instant'] = False
    
            for dest in destinations:
                if self.oneQueue:
                    tableDest = 'ms_message_buffer_in'
                else:
                    tableDest = 'ms_message_'+dest[1]+'_buffer_in'
                msg = {'type':str(typeid), 'source':str(self.procid), \
                       'dest':str(dest[0]), 'payload':str(payload), \
                       'delay':str(delay)}
                if message['instant']:
                    if not self.instantMsg.has_key(tableDest):
                        self.instantMsg[tableDest] = []  
                    self.instantMsg[tableDest].append(msg)
                else:
                    if not self.msg.has_key(tableDest):
                        self.msg[tableDest] = []  
                    self.msg[tableDest].append(msg)

            for dest in priorityDestinations:
                if self.oneQueue:
                    tableDest = 'ms_priority_message_buffer_in'
                else:
                    tableDest = 'ms_priority_message_'+dest[1]+'_buffer_in'
                msg = {'type':str(typeid), 'source':str(self.procid), \
                       'dest':str(dest[0]), 'payload':str(payload), \
                       'delay':str(delay)}
                if message['instant']:
                    if not self.instantPriorityMsg.has_key(tableDest):
                        self.instantPriorityMsg[tableDest] = []  
                    self.instantPriorityMsg[tableDest].append(msg)
                else:
                    if not self.priorityMsg.has_key(tableDest):
                        self.priorityMsg[tableDest] = []  
                    self.priorityMsg[tableDest].append(msg)

        # deliver messages that need to be delivered immediately.
        self.deliver(instant = True)

    def deliver(self, instant = False):
        """
        __deliver__

        Delivers the messages when the finish method is called, or when 
        messages need to be delivered instantantly when published.
        """
        # set the message has arrive table entry to true for the destinations.

        # check which type of messages we need to deliver
        if instant:
            msg = self.instantMsg
            priority = self.instantPriorityMsg
        else:
            msg = self.msg
            priority = self.priorityMsg

        # update the metadata that components can use before looking
        # into the big table. As we use dictionaries of messages that map
        # the table to the messages we do not need to distinguish between
        # one or more queues.
        dest = {}
        for tableDest in msg.keys():
            for ms in msg[tableDest]:
                dest[ms['dest']] = 'update'
        args = {'table' : 'ms_available', 'msgs' : dest}
        if len(dest)>0:
            self.query.msgArrived(args = args)

        dest = {}
        for tableDest in priority.keys():
            for ms in priority[tableDest]:
                dest[ms['dest']] = 'update'
        args = {'table' : 'ms_available_priority', 'msgs' : dest}
        if len(dest)>0:
            self.query.msgArrived(args = args)
 
        # after inserting messages we need to check if certain buffers
        # are full and purge/move data.

        checkBuffers = ['ms_history_buffer', 'ms_history_priority_buffer']
        # update the actual message tables/buffers.
        for tableDest in msg.keys():
            checkBuffers.append(tableDest)
            if len(msg[tableDest])>0: 
                args = {'table' : tableDest, 'msgs' : msg[tableDest]}
                self.query.insertMsg(args = args)
                args = {'table' : 'ms_history_buffer', 'msgs' : msg[tableDest]}
                self.query.insertMsg(args = args)

        for tableDest in priority.keys(): 
            if len(priority[tableDest])>0:
                args = {'table' : tableDest, 'msgs' : priority[tableDest]}
                self.query.insertMsg(args = args)
                args = {'table' : 'ms_history_priority_buffer', \
                        'msgs' : priority[tableDest]}
                self.query.insertMsg(args = args)

        # check if we need to emtpty the buffer.
        logging.debug("Checking if following buffers are full: " \
            + str(checkBuffers))
        for tableName in checkBuffers:
            bufferSize = self.query.tableSize(args = tableName)
            if bufferSize > self.bufferSize:
                logging.debug("Buffer full for: "+tableName+". Purging")
                target = tableName[0:tableName.find("_buffer")]
                args = {'source' : tableName, 'target' : target}
                logging.debug("moving messages: " + str(args))
                self.query.moveMsgFromBufferIn(args = args)
        if instant:
            self.instantMsg = {}
            self.instantPriorityMsg = {}
        else:
            self.msg = {}
            self.priorityMsg = {}

    
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
        # logging
        logging.debug("publish unique requested")
        if type(args) == dict:
            args = [args]

        # find the tables we need to check first.
        tableNames = []
        tables = self.query.showTables()
        for table in tables:
            if table[0].rfind('ms_message') == 0:
                tableNames.append(table[0])  
            if table[0].rfind('ms_priority_message') == 0:
                tableNames.append(table[0])  

        # find message types first
        messageTypes = {}
        for message in args:
            messageTypes[message['name']] = {}

        noGo = {}
        for messageType in messageTypes.keys():
            # check if message type is in database
            result = self.query.checkMessageType(args = {'name' : messageType})
            # get message type id
            if result != {}:
                # message type was registered before, get id
                messageTypes[messageType]['typeid'] = result['typeid']
            else:
                self.query.insertMessageType({'name' : messageType})
                # get id
                typeid = self.query.lastInsertId()
                messageTypes[messageType]['typeid'] = typeid

            # check if this message is still in the queue(s)
            for tableName in tableNames:
                arg = {'tableName' : tableName, \
                    'sqlArgs' : {'typeid': messageTypes[messageType]['typeid']}}
                nrOfMsg = self.query.inQueue(arg)
                if nrOfMsg > 0:
                    noGo[messageType] = 'no_go'
                    break 

        # filter out the messages that have a no go and pass on the others.
        messageGo = []
        for message in args:
            if not message['name'] in noGo.keys():      
                messageGo.append(message)
        # publish unique messages.
        self.publish(messageGo)

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
        # check if there are any unfinished messages we want to get them first.
         # logging
        logging.debug("Get requested for: "+self.name)
        # start looking in the correct tables.
        bufferIn = 'ms_message_buffer_in'
        queue = 'ms_message'
        bufferOut = 'ms_message_buffer_out'
        priorityBufferIn = 'ms_priority_message_buffer_in'
        priorityQueue = 'ms_priority_message'
        priorityBufferOut = 'ms_priority_message_buffer_out'

        if not self.oneQueue: 
            queue = 'ms_message_'+self.name
            bufferIn = 'ms_message_'+self.name+'_buffer_in'
            bufferOut = 'ms_message_'+self.name+'_buffer_out'
            priorityBufferIn = 'ms_priority_message_'+self.name+'_buffer_in'
            priorityQueue = 'ms_priority_message_'+self.name
            priorityBufferOut = 'ms_priority_message_'+self.name+'_buffer_out'

        message = None
        while True:
            logging.debug("Checking messages for: "+self.name)
            # check if there are any messages at all for us:
            args = {'table':'ms_available_priority', 'procid':self.procid}
            result = self.query.msgAvailable(args)
            # there is a message, lets look for it
            if result[0] == 'there':
                self.currentMsgTable = priorityBufferOut
                # try buffer out.
                message = self.query.getMsg({'table':priorityBufferOut, \
                                             'procid':self.procid}) 
                if message != {}:
                    break 
                #nothing from buffer, move (if any) from big table to buffer_out
                args = {'source': priorityQueue, \
                        'target':priorityBufferOut, \
                        'procid':self.procid, \
                        'buffer_size':self.bufferSize}
                self.query.moveMsgToBufferOut(args)
                message = self.query.getMsg({'table':priorityBufferOut, \
                                             'procid':self.procid}) 
                if message != {}:
                    break 
                args = {'source': priorityBufferIn, \
                        'target':priorityBufferOut, \
                        'procid':self.procid, \
                        'buffer_size':self.bufferSize}
                self.query.moveMsgToBufferOut(args)
                message = self.query.getMsg({'table':priorityBufferOut, \
                                             'procid':self.procid}) 
                if message != {}:
                    break 
                # there was a message but we did not find it, this means 
                # it can be a delayed message. If we know the minimum 
                # delay we can
                # wait that long. For the moment we assume that there 
                # is a continous stream of messages to a component.
                args = {'table':'ms_available_priority', 'procid':self.procid}
                self.query.noMsgs(args)
            args = {'table':'ms_available', 'procid':self.procid}
            result = self.query.msgAvailable(args)
            # there is a message, lets look for it
            if result[0] == 'there':
                # try buffer out.
                self.currentMsgTable = bufferOut
                message = self.query.getMsg({'table':bufferOut, \
                                             'procid':self.procid}) 
                if message != {}:
                    break 
                #nothing from buffer, move (if any) from big table to buffer_out
                args = {'source': queue, 'target':bufferOut, \
                        'procid':self.procid, 'buffer_size':self.bufferSize}
                self.query.moveMsgToBufferOut(args)
                message = self.query.getMsg({'table':bufferOut, \
                                             'procid':self.procid}) 
                if message != {}:
                    break 
                args = {'source': bufferIn, 'target':bufferOut, \
                        'procid':self.procid, 'buffer_size':self.bufferSize}
                self.query.moveMsgToBufferOut(args)
                message = self.query.getMsg({'table':bufferOut, \
                                             'procid':self.procid}) 
                if message != {}:
                    break 
                # there was a message but we did not find it, this means it 
                # can be a delayed message. If we know the minimum 
                # delay we can
                # wait that long. For the moment we assume that there is a 
                # continous stream of messages to a component.
                args = {'table':'ms_available', 'procid':self.procid}
                self.query.noMsgs(args)

            if not wait:
                # return immediately with no message
                return (None, None)
            logging.debug("Sleeping "+str(self.pollTime)+ \
                " seconds and check for messages again")
            time.sleep(self.pollTime)      

        self.currentMsg = message

        args = {'table' : self.currentMsgTable, 'msgId' : message['messageid']}
        # change state message
        self.query.processMsg(args)
        # add destination for testing purposes only
        message['target'] = self.name
        # return message
        
        return message


    
    ##########################################################################
    # finish method 
    ##########################################################################

    def finish(self):
        """
        __finish__
        
        called after the messages has been handled. this is to prevent long open
        connections underlying databases without committing. 
        
        MsgService.get()
        ...<do your potentially long standing operations>...
        MsgService.finish()
        """
        # publish unpublished messages.
        self.deliver()
        # check if the history tables are not too large
        self.cleanHistory()
        # remove the message from the queu that we where working on.
        if self.currentMsgTable != None and self.currentMsg != None:
            args = {'table' : self.currentMsgTable, \
                    'msgId' : self.currentMsg['messageid']}
            self.query.removeMsg(args)
            self.currentMsgTable = None
            self.currentMsg = None

    ##########################################################################
    # purgeMessages method 
    ##########################################################################

    def purgeMessages(self):
        """
        __purgeMessages__
        
        Drop all messages to be delivered. 
        """
        tableNames = []
        tables = self.query.showTables()
        for table in tables:
            if table[0].rfind('ms_message') == 0:
                tableNames.append(table[0])  
            if table[0].rfind('ms_priority_message') == 0:
                tableNames.append(table[0])  
        for table in tableNames:
            self.query.purgeTable(table)        

    ##########################################################################
    # remove messages of a certain time addressed to me
    ##########################################################################

    def remove(self, messageType):
        """
        __remove__

        Remove all messages of a certain type addressed to me.
        """
        self.oneQueue = True 

        # logging
        logging.info("Remove messages of type %s." % messageType)
        # get message type (if it is in database)
        result = self.query.checkMessageType(args = {'name' : messageType})
        # no rows, nothing to do
        if result == {}:
            return
        tables = []
        if self.oneQueue:
            tables = ['ms_message', 'ms_message_buffer_in', \
                      'ms_message_buffer_out', 'ms_priority_message', \
                      'ms_priority_message_buffer_in', \
                      'ms_priority_message_buffer_out']
        else:
            tables = ['ms_message_'+self.name, \
                      'ms_message_'+self.name+'_buffer_in', \
                      'ms_message_'+self.name+'_buffer_out', \
                      'ms_priority_message_'+self.name, \
                      'ms_priority_message_'+self.name+'_buffer_in', \
                      'ms_priority_message_'+self.name+'_buffer_out']
        for table in tables:
            args = {'tablename' : table, \
                'sqlArgs' : {'typeid' : result['typeid'], \
                             'procid' : str(self.procid)}}
            self.query.removeMessageType(args = args)

    ##########################################################################
    # remove messages in history
    ##########################################################################

    def cleanHistory(self, maxSize = 0):
        """
        __cleanHistory__
        
        Delete history messages older than the number of hours
        specified.
        
        """
        if maxSize == 0:
            maxSize = self.historySize
        for table in ['ms_history', 'ms_history_priority']:
            size = self.query.tableSize(args = table)
            if size > maxSize:
                newSize = min(self.historyMin, size)
                if newSize > 0:
                    args = {'table' : table}
                    maxId = self.query.maxId(args = args)[0]
                    args = {'table' : table, 'maxId' : maxId-newSize}
                    self.query.purgeHistory(args = args)
     
    def subscriptions(self):
        """
        __subscriptions__

        Returns a list (array) of subscriptions
        """
        return self.query.subscriptions(args = {'procid' : self.procid}) 

    def prioritySubscriptions(self):
        """
        __prioritySubscriptions__

        Returns a list (array) of subscriptions for priority messages
        """
        return self.query.prioritySubscriptions({'procid' : self.procid}) 

    def pendingMsgs(self, componentName = None):
        """
        __pendingMsgs__
  
        Returns the number of messages that are pending (total)
        Component name is only relevant if multi queue is used.
        """
        pending = 0 
        tableNames = []
        if componentName == None:
            tables = self.query.showTables()
            for table in tables:
                if table[0].rfind('ms_message') == 0:
                    tableNames.append(table[0])  
                if table[0].rfind('ms_priority_message') == 0:
                    tableNames.append(table[0])  
        else:
            tableNames = ['ms_message_'+componentName, \
                          'ms_message_'+componentName+'_buffer_in', \
                          'ms_message_'+componentName+'_buffer_out'] 
        for tableName in tableNames: 
            pending += self.query.tableSize(args = tableName)
        return pending
