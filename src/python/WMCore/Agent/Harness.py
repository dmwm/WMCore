#!/usr/bin/env python
"""
Harness class that wraps standard functionality used in all deamon 
components including:

(1) Instantiating message service, trigger and other database classes
including session objects and workflow entities.

(2) Subscribing to default messages (e.g. logging and stopping server)

(3) Providing a method to test the component without a message service

(4) Instatiate the log master

(5) Provides basic uniform log information (useful for operators)

(6) Method to publish monitoring information

"""

__revision__ = "$Id: Harness.py,v 1.2 2008/09/04 12:31:24 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "fvlingen@caltech.edu"

from logging.handlers import RotatingFileHandler

import logging
import os
import threading


from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION
from WMCore.WMFactory import WMFactory

class Harness:
    """
    Harness class that wraps standard functionality used in all deamon 
    components
    """

    def __init__(self, **args):
        """
        init

        Default intialization of the harness including setting some diagnostic
        messages
        """
        try:
            self.state = 'startup'
            self.args = {}
            self.messages = {}
            self.args['logFile'] = None
            self.args.update(args)
            # the component name is the last part of its module name
            # and it should override any name give through the arguments.
            self.args['componentName'] = str(self.__class__.__name__)
            self.args['componentDir'] = os.path.join(self.args['workDir'], \
                self.args['componentName'])
            if self.args['logFile'] == None:
                self.args['logFile'] = os.path.join(self.args['componentDir'], \
                    "ComponentLog")
            # we have name and location of the log files. Now make sure there
            # is a directory.
            try:
                os.makedirs(self.args['componentDir'])
            except :
                print('component dir already exists. Moving on')
            logHandler = RotatingFileHandler(self.args['logFile'],
                "a", 1000000, 3)
            logFormatter = \
                logging.Formatter("%(asctime)s:%(module)s:%(message)s")
            logHandler.setFormatter(logFormatter)
            logging.getLogger().addHandler(logHandler)
            logging.getLogger().setLevel(logging.INFO)
            logging.info(">>>Starting: "+self.args['componentName']+'<<<')
            # check which backend to use: MySQL, Oracle, etc... for core 
            # services.
            if not self.args.has_key('db_dialect'):
                raise WMException(WMEXCEPTION['WMCORE-5'],'WMCORE-5')
            myThread = threading.currentThread()
            logging.info(">>>Determining Dialect: "+self.args['db_dialect'])
            if self.args['db_dialect'] == 'mysql':
                myThread.dialect = 'MySQL'
            myThread.logger = logging.getLogger()
            logging.info(">>>Setting arguments for main thread: "+ \
                str(self.args))
            myThread.args = self.args
            logging.info(">>>Initializing Msgservice Factory")
            WMFactory("msgService", "WMCore.MsgService."+ \
                myThread.dialect)
            myThread.msgService = \
                myThread.factory['msgService'].loadObject("MsgService")

            # we recognize there can be more than one database.
            # be we offer a default database that is used for core services.
            logging.info(">>>Initializing default database")
            logging.info(">>>Check if connection is through socket")
            options = {}
            if self.args.has_key("db_socket"):
                options['unix_socket'] = self.args['db_socket']
            logging.info(">>>Building database connection string")
            dbStr = self.args['db_dialect'] + '://' + self.args['db_user'] + \
                ':' + self.args['db_pass']+"@"+self.args['db_hostname']+'/'+\
                self.args['db_name']
            # we only want one DBFactory per database so we will need to 
            # to pass this on in case we are using threads.
            myThread.dbFactory = DBFactory(myThread.logger, dbStr, options)
            myThread.dbi = myThread.dbFactory.connect()
            logging.info(">>>Initialize transaction dictionary")
            myThread.transactions = {}
            # diagnostic messages are ones that most of the time
            # bypass the other messages. 
            logging.info(">>>Initializing diagnostic messages")
            self.diagnosticMessages = []
            # can be used to print out the parameter 
            # and handlers used by an agent
            self.diagnosticMessages.append(self.args['componentName']+\
                ':LogState')
            self.diagnosticMessages.append('LogState')
            # start/stop debug
            self.diagnosticMessages.append(self.args['componentName']+\
                ':Logging.DEBUG')
            self.diagnosticMessages.append(self.args['componentName']+\
                ':Logging.INFO')
            self.diagnosticMessages.append(self.args['componentName']+\
                ':Logging.ERROR')
            self.diagnosticMessages.append('Logging.DEBUG')
            self.diagnosticMessages.append('Logging.INFO')
            self.diagnosticMessages.append('Logging.ERROR')
            # events to stop the component.
            self.diagnosticMessages.append(self.args['componentName']+\
                ':Stop')
            self.diagnosticMessages.append('Stop')


            logging.info(">>>Instantiating trigger service")
            #FIXME: add when trigger service is ready
            logging.info("Harness part constructor finished")
        except Exception,ex:
            logging.critical("Problem instantiating "+str(ex))
            raise

    def preInitialization(self):
        """
        _preInitialization_
 
        returns: nothing
        
        method that can be overloaded and will be called before the 
        start component is called. (enables you to set message->handler 
        mappings). You use the self.message dictionary of the base class
        to define the mappings.

        """
        pass

    def postInitialization(self):
        """
        _postInitialization_
 
        returns: nothing
        
        method that can be overloaded and will be called after the start 
        component does the standard initialization, but before the wait 
        (enables you to publish events when starting up)

        Define actions you want to execute before the actual message
        handling starts. E.g.: publishing some messages, or removing
        messages.

        """
        pass

    def logState(self):
        """
        _logState_
 
        returns: string
        
        method that can be overloaded to log additional state information 
        (should return atring)
        """
        msg = 'No additional state information for ' + \
            self.args['componentName']
        return msg

    def publishItem(self, items = {}):
        """
        _publishItem_

        returns: nothing
  
        A method that publishes a (dictionary) set or 1 item
        to a monitoring service.
        """
        #FIXME: do we need this method. If so we need to agree
        #FIXME: on some default monitoring publication mechanism.
        pass

    def __call__(self, event, payload):
        """
        Loads the correct handler and performs the apropiate actions
        """
        # check if it is not a diagnostic event.
        if not event in self.diagnosticMessages:
            if not event in self.messages.keys():
                msg = """
Message %s with payload: %s has no handler in this component.
I am going to throw a fatal error! The following message subscriptions 
which have a handler, have been found: diagnostic: %s and component specific: %s 
                """ % (event, payload, str(self.diagnosticMessages), \
                str(self.messages.keys()))
                logging.critical(msg)
                raise Exception(msg)
            handler = self.messages[event]
            logging.debug("Retrieving Handler for event: "+event)
            logging.debug("Executing Payload " + str(payload))
            handler.__call__(event, payload)
            logging.debug("Event " + str(event) + " successfully handled")
        # diagnostics are tiny operations so we put them here rather 
        # than in separate handlers
        else:
            if(event == self.args['componentName']+':Logging.DEBUG') or \
                (event == 'Logging.DEBUG'):
                logging.getLogger().setLevel(logging.DEBUG)
                logging.info("Log level set to DEBUG")
            elif(event == self.args['componentName']+':Logging.INFO') or \
                (event == 'Logging.INFO'):
                logging.getLogger().setLevel(logging.INFO)
                logging.info("Log level set to INFO")
            elif(event == self.args['componentName']+':Logging.ERROR') or \
                (event == 'Logging.ERROR'):
                logging.getLogger().setLevel(logging.ERROR)
                logging.info("Log level set to ERROR")
            elif(event == self.args['componentName']+':LogState') or \
                (event == 'LogState'):
                logging.info(str(self))

    def initialization(self):
        """
        _initialization__

        Performs the basic initialization. e.g.:
        
        - registering the trigger and message service with handlers
        - checking if the component specific message types conflict
        with the default diagnostic ones
        - registering itself with the message service
        - subscribing to message types.
        """
 
        try:
  
            myThread = threading.currentThread()
            # register this component
            logging.info(">>>Registering this component to msgService")
            myThread.msgService.registerAs(self.args['componentName'])
            logging.info(">>>Subscribing to events:")
            # subscribe to messages (or generate a warning:
            if len(self.messages.keys())==0:
                logging.warning("COMPONENT DOES NOT SUBSCRIBE TO MESSAGES!")
                logging.warning("IS THIS INTENTIONAL?!")
            for message in self.messages.keys():
                # check if the messages do not conflict with our 
                # diagnostic ones
                if message in self.diagnosticMessages:
                    raise WMException(WMEXCEPTION['WMCORE-6'], 'WMCORE-6')
                myThread.msgService.subscribeTo(message)
                logging.info(">>>Subscribed to event : " + message)
            logging.info(">>>Subscribing to diagnostic events : ")
            for message in self.diagnosticMessages:
                myThread.msgService.prioritySubscribeTo(message)
                logging.info(">>>Subscribed to event : " + message)
            # remove any stop messages that where send to us
            # while we where not running
            logging.info(">>>Before I start I purge any stop messages send "+\
                "to me")
            myThread.msgService.remove("Stop")
            myThread.msgService.remove(self.args["componentName"]+":Stop")
        except Exception,ex:
            logging.critical("Prolem initializing : "+str(ex))
            raise

    def prepareToStart(self):
        """
        _prepareToStart_

        returns: Nothing

        Starts the initialization procedure. It is mainly an aggregation method
        so it can easily used in tests.
        """
        self.state = 'initialize'
        type = None
        payload = None
        # note: every component gets a (unique) name: 
        # self.args['componentName']
        logging.info('>>>Starting initialization\n')

        logging.info('>>>Setting default transaction')
        myThread = threading.currentThread()
        myThread.transaction = Transaction(myThread.dbi)

        self.preInitialization()
        self.initialization()
        self.postInitialization()

        logging.info('>>>Committing default transaction')
        myThread.transaction.commit()

        logging.info('>>>Committing possible other transactions')
        # if we have multiple database we might want to synchronize
        # commits
        for transaction in myThread.transactions.keys():
            transaction.commit()

        logging.info(">>>Initialization finished!\n")    
        # wait for messages
        self.state = 'active'


    def handleMessage(self, type, payload):
        """
        __handleMessage_

        A direct method for handling events for this component.
        This method is mainly used for testing frameworks where you want to 
        have immediate feedback on the handling of a message in the test framework.
        """ 
        logging.debug("Receiving message of type: "+str(type)+\
        ", payload: "+str(payload))
         # check if it is a stop message:
        if type == 'Stop' or type == self.args['componentName']+':Stop':
            return
        self.__call__(type, payload)

    def startComponent(self):
        """
        _startComponent_

        returns: Nothing

        Start up the component, performs initialization an waits 
        for messages.
 
        """
        myThread = threading.currentThread()
        try:
            self.prepareToStart()
            # wait for messages
            myThread.transaction = Transaction(myThread.dbi)
            while True:
                myThread.transaction.begin()
                type, payload = myThread.msgService.get()
                # we commit here as we do not want long standing open 
                # database connections (but we keep track of the last get 
                # message state
                myThread.transaction.commit()
                self.handleMessage(type, payload)
                logging.debug(">>>Closing and commit all database sessions" \
                    +" that have been registered")
                # when we call the msgService.finish we finally remove the msg
                # from the queu.
                myThread.transaction.begin()
                myThread.msgService.finish()
                myThread.transaction.commit()
                for transaction in myThread.transactions.keys():
                    transaction.commit()
                logging.debug(">>>Finished handling message of type "+ \
                    str(type)+ " \n")
                if type == 'Stop' or type == self.args['componentName']+':Stop':
                    logging.info(">>>Gracefully shutting down component")
                    break
        except Exception,ex:
            logging.info(\
                ">>>Fatal error, rollback all non-committed transactions")
            logging.info(">>>Closing all connections")
            for transaction in myThread.transactions.keys():
                transaction.rollback()
            if self.state == 'initialize':
                msg = """ 
PostMortem: choked when initializing with error: %s
                """ % (str(ex))
            else:
                msg = """ 
PostMortem: choked while handling messages  with error: %s
while trying to handle event/payload: %s/%s
                """ % (str(ex), type, payload)
            logging.critical(msg)
            raise
        logging.info("System shutdown complete!")

    def __str__(self):
        """

        return: string
 
        String representation of the status of this component.
        """
        
        msg = 'Status of this component : \n'
        msg += '\n'
        msg += '>>Event Subscriptions --> Handlers<<\n' 
        msg += '------------------------------------\n'
        for message in self.messages.keys():
            msg += message+'-->'+ str(self.messages[message])+'\n'
        msg += '\n'
        msg += '\n'
        msg += '>>Parameters --> Values<<\n'
        msg += '-------------------------\n'
        for parameter in self.args.keys():
            msg += parameter+'-->'+str(self.args[parameter])+'\n\n'
        additionalMsg = self.logState()
        if additionalMsg != '':
            msg += '\n'
            msg += 'Additional state information\n'
            msg += '----------------------------\n'
            msg += '\n'
            msg += str(additionalMsg)
            msg += '\n'
        return msg
   

