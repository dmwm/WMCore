#!/usr/bin/env python

"""
_RemoteMsg_

This module offers a way for remote message exchange. This is not the same
as the standard WMCore message service, in which a shared database is used
for communication. Current implementation is based on a HTTP server that 
interprets requests as messages.

All messages will be composed of a message type and a payload. This class can 
be used as a library and is capable of acting both as server and client. It 
must be configured with a WMCore.Configuration object. Have a look at the 
SampleConfiguration class for an example.

A single object of the RemoteMsg class offers the whole interface. By calling
the 'publish' method, messages are sent to pre-configured adresses (of remote
objects of RemoteMsg class). User/password authentication will be used if
indicated in the configuration (in both client and server). 

When the 'startListener' method is invoked, a server that listens for incoming 
messages of remote ends is started. If in queue mode, the received messages 
will be stored  in a queue for later retrieval (via the 'get' method). 
If not in queue mode,  the messages will be handled by callable objects, as 
mapped previously by use of the 'setHandler' method (as an example look at
the SimpleHandler class). The handlers may give some result back to the caller.
This will be retrieved as return value of the 'publish' method if the caller
used the 'sync' flag, otherwise only a brief acknowledgement of the reception
of the message will be returned (or an error message) without waiting for the
execution of the corresponding handler.

This library has been designed to be compatible with WMCore components. It can
be used inside one although this is not a requirement (can be used by any other
program). If used inside a WMCore component, the DB interface is made available
to handlers, so they can use it to access the tables of its component. For more
information look at the examples at the SimpleInCompHandler class.

Example of use in a standalone script:
---------------------------------------------------------
 from WMCore import Configuration
 from RemoteMsg.RemoteMsg import RemoteMsg

 config = Configuration.loadConfigurationFile("myConf.py")

 # Start the remote msg service (started in __init__, destroyed in __del__)
 remoteMsg = RemoteMsg(config, queue = False)
 remoteMsg.setHandler('type1', 'RemoteMsg.SimpleHandler')
 remoteMsg.setHandler('type2', 'RemoteMsg.SimpleHandler')

 # Let's communicate with another server in same machine (but different port)
 remoteMsg.setAddress(['127.0.0.1:8010'])

 # Start the listener
 remoteMsg.startListener()

 # Publish a message
 try:
    print remoteMsg.publish("type1", "Sending a message of type1 from A", \
       sync = False)
    print remoteMsg.publish("type2", "Sending a message of type2 from A", \
       sync = True)

 except Exception, inst:
    print "Exception in publish: %s" % inst

 time.sleep(4)

 # Print messages in queue ==> Empty, as we set queue = False
 print
 print "Printing messages in queue of A:"
 msg = remoteMsg.get()
 while msg != None:
    print "   -> msg: %s" % msg
    msg = remoteMsg.get()
---------------------------------------------------------


Example of use in a WMCore Component:
---------------------------------------------------------
from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory
from RemoteMsg.RemoteMsg import RemoteMsg
import logging

class MyComponent(Harness):
    def __init__(self, config):
      Harness.__init__(self, config)
      print (config)
      
    def preInitialization(self):
      # Add message system handlers
      #  ...
      # These are normal WMCore handlers, not RemoteMsg handlers
      # They will also be able to publish remotely by using:
      #   self.component.remoteMsg.publish(...)

    def postInitialization(self):
	   # Start RemoteMsg and add some handlers for remote messages		    
      # Those handlers will also have access to component DB
      self.remoteMsg = RemoteMsg(self.config)
      self.remoteMsg.setQueue(False)
      self.remoteMsg.setHandler('type1', 'MyComp.SomeHandler')
      self.remoteMsg.startListener()

    def prepareToStop(self, wait = False, stopPayload = ""):
      # Destroy (stop) the Remote message instance
      self.remoteMsg.__del__()
      Harness.prepareToStop(self, wait, stopPayload)
---------------------------------------------------------
"""

__revision__ = "$Id: RemoteMsg.py,v 1.1 2009/04/01 12:56:01 delgadop Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "antonio.delgado.peris@cern.ch"

import os
import threading
from logging.handlers import RotatingFileHandler

from WMCore.WMFactory import WMFactory

# Classes from this package
from Sender import Sender
from Listener import Listener
from HttpTree import HttpTree

#for logging
import logging


class RemoteMsg(object):
    """ 
    _RemoteMsg_ 
 
    Main interface of the RemoteMsg module. Clients wishing to use the 
    RemoteMsg should instantiate an object of this class and interface 
    it using the public methods declared by it.
    
    """
    def __init__(self, config, addresses = [], queue = True):
        """
        Constructor.
        
        Requires a WMCore.Configuration object with configuration 
        information. The addresses of recipients and the flag for 
        queue/handling mode can be set with setAdress or setQueue methods 
        also (have a look at their docstring for further help). The 
        listener for messages needs to be started with the startListener 
        method, meanwhile only publication capabilities are available.
        """
        self.myconfig = config
        
        sections = self.myconfig.listSections_()
        if not "RemoteMsg" in sections:
            msg = "Cannot create RemoteMsg object without "
            msg += "RemoteMsg section in config file"
            raise Exception(msg)
       
        self.mylogger = None
        self.logMsg = None
        self.__setLogging__()
        self.mylogger.info("\n\n>>>>>RemoteMsg object being created <<<<<<\n")
        
        self.myComp = None
        if hasattr(self.myconfig.RemoteMsg, "inComponent"):
            self.myComp = self.myconfig.RemoteMsg.inComponent
        
        self.queueMode = queue
        self.addresses = addresses
        self.user = None
        self.passwd = None
        
        self.msgLock = threading.Lock()
        self.handlerLock = threading.Lock()
        self.factory = WMFactory('RemoteMsg')
        
        # If this is instantiated by a WMCore component, get its DB interface
        if self.myComp:
            # Get a reference to our invoking component's DB factory
            myThread = threading.currentThread()
            self.dbFactory = myThread.dbFactory
            self.dialect = myThread.dialect

        # TODO: Our msg queue is just in memo for now, but it might be in a DB
        # We already have the DB interface, but we would need our own Create
        # and Queries modules (in principle, different from those of the comp)
#         self.factory = WMFactory("threadPool", "WMCore.ThreadPool."+ \
#                              myThread.dialect)
#         self.queries = factory.loadObject(self.myComp+"Database"+ \
#                                        myThread.dialect+"Queries")

        self.msgQueue = []
        self.handlerMap = {}
       
       
        # Formatter for responses (change only if in synchronous mode)
        formatter = "RemoteMsgComp.DefaultFormatter"
        if hasattr(self.myconfig.RemoteMsg, "formatter"):
            formatter = self.myconfig.RemoteMsg.formatter
        formatterObj = self.factory.loadObject(formatter)
       
        params = { "msgQueue": self.msgQueue,   
                   "handlerMap": self.handlerMap,
                   "msgLock": self.msgLock,     
                   "formatter": formatterObj, 
                   "queueMode": self.queueMode 
                 }
       
        if self.myComp:
            params["component"] = self.myComp
            params["dbFactory"] = self.dbFactory
            params["dialect"] = self.dialect
            
        self.httpTree = HttpTree(params)
       
        self.sender = None
        self.__createSender__()
        self.listener = None
          


    def __del__(self):
        # Tell cherrypy to die
        self.mylogger.info("Asking listener to die")
        if self.listener:
            self.listener.terminate()
            self.listener.join()



    def __setLogging__(self):
        """
        Initializes logging. Use by the constructor.
        """
        compSect = self.myconfig.RemoteMsg
 
        # Logging
        if not hasattr(compSect, "logFile"):
            compSect.logFile = os.path.join(compSect.RemoteMsgDir, \
                "remoteMsg.log")
        print('Log file is: '+compSect.logFile)
 
        if not hasattr(compSect, "listenerLogFile"):
            compSect.listenerLogFile = os.path.join(compSect.RemoteMsgDir, \
                "listener.log")
        print('Listener log file is: '+compSect.listenerLogFile)
 
        logHandler = RotatingFileHandler(compSect.logFile,
            "a", 1000000, 3)
        logFormatter = \
            logging.Formatter("%(asctime)s:%(levelname)s:%(filename)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        self.mylogger = logging.getLogger("RemoteMsg")
        self.mylogger.addHandler(logHandler)
        self.mylogger.setLevel(logging.INFO)
        # map log strings to integer levels:
        self.logMsg = {'DEBUG' :   logging.DEBUG,    \
                     'ERROR' :   logging.ERROR,     \
                     'NOTSET':   logging.NOTSET,    \
                     'CRITICAL' : logging.CRITICAL, \
                     'WARNING'  : logging.WARNING,  \
                     'INFO'     : logging.INFO,     }
##                    'SQLDEBUG' : logging.SQLDEBUG  }
        if hasattr(compSect, "logLevel") and \
            compSect.logLevel in self.logMsg.keys():
            self.mylogger.setLevel(self.logMsg[compSect.logLevel])


    def __createSender__(self):
        """
        Initializes the sender object. Used by the constructor.
        """
   
        # Sender is not a new thread, so it does not need a lock
        params = {}
        params['addresses'] = self.addresses
        params['port'] = "8030"
        if hasattr(self.myconfig.RemoteMsg, "senderPort"):
            params['port'] = self.myconfig.RemoteMsg.senderPort
        params['service'] = "msg"
        if hasattr(self.myconfig.RemoteMsg, "senderService"):
            params['service'] = self.myconfig.RemoteMsg.senderService
        params['user'] = None
        if hasattr(self.myconfig.RemoteMsg, "senderUser"):
            params['user'] = self.myconfig.RemoteMsg.senderUser
        params['pwd'] = None
        if hasattr(self.myconfig.RemoteMsg, "senderPwd"):
            params['pwd'] = self.myconfig.RemoteMsg.senderPwd
        params['realm'] = 'RemoteMsg'
        if hasattr(self.myconfig.RemoteMsg, "realm"):
            params['realm'] = self.myconfig.RemoteMsg.senderRealm
 
        self.sender = Sender(self.msgQueue, params) 
 
 
 
    def startListener(self):
        """
        Starts a listener for incoming remote messages. The method will refuse
        to create a listener if there is already one
        """
        if not self.listener:
            self.mylogger.info("Starting listener")
            self.listener = Listener(self.httpTree, self.myconfig)
            self.listener.start()         
        else:
            msg = "Refusing to start listener (there is already one)."
            self.mylogger.info(msg)
    

    def setAddress(self, addresses):
        """
        Sets the addresses of the remote ends. Argument should be a list of 
        IPs or hostnames. The publish method will send messages to all members
        of this list.
        """
        self.addresses = addresses
        self.sender.setAddress(self.addresses)


    def setAuthentication(self, user, passwd):
        """
        Sets the user/password for authentication
        with the remote application. Has to be done
        before sending a message.
        """
        self.mylogger.debug("Setting user and passwd")
        self.user = user
        self.passwd = passwd

    def setQueue(self, value):
        """
        This is an option that either allows messages that are being received
        to be put in a local queue (so a get method can retrieve them), or if
        set to False  messages are handled directly through the handler framework.
        """
        self.queueMode = value
        self.httpTree.setQueue(self.queueMode)

    def get(self):
        """
        Gets messages from the local buffer (those not handled as explained 
        in the setQueue method).
        The first message of the queue is retrieved and returned. This method
        is only used when the queue is set to True. If queue is set to False
        or there are no stored messages, None is returned.
        """
        if self.queueMode:
            self.msgLock.acquire()
            if self.msgQueue:
                result = self.msgQueue.pop(0)
            else:
                result = None
            self.msgLock.release()
            return result
        else:
            return None


    def publish(self, msgType, payload, sync = False):
        """
        Sends a message to the remote end. 
     
        If sync is set to True, the remote server will complete the message
        handling before replying with some generated data. Otherwise, the 
        remote end will immediately reply with some "Message received"
        indication and execute the handler asynchronously (if the remote end
        is in queue mode, there is no handler execution, so this flag is 
        meaningless).
        
        In any case, the response of the message (e.g. a json string product 
        of the handling of the HTTP request) is returned. 
     
        Can throw an HTTP exception in case of error connecting with the remote
        end.
        """
        return self.sender.send(msgType, payload, sync)


    def setHandler(self, msgType, handler):
        """
        Maps the specified handler to the indicated message type.
        The handler must be the name of a class which can be called (e.g.
        RemoteMsg.SimpleHandler). The handler will only be called if
        queue mode is set to False.
        """
        msg = "Setting new handler %s for msgType %s" % (handler, msgType)
        self.mylogger.debug(msg)
        #  Factory to dynamically load handlers
        params = {}
        if self.myComp:
            params["component"] = self.myComp
        newHandler = self.factory.loadObject(handler, params)
        self.handlerLock.acquire()
        self.handlerMap[msgType] = newHandler
        self.handlerLock.release()

