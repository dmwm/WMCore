#!/usr/bin/env python
#pylint: disable-msg=E1101,C0103,R0902
"""
MsgServiceApp does sopme standard setup and teardown for the MsgService
"""

__revision__ = "$Id: MsgServiceApp.py,v 1.1 2008/10/30 02:32:03 rpw Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "rickw@caltech.edu"

import commands
import logging
import os
import threading
import time
import unittest


from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory

class MsgServiceApp():
    """
    TestCase for modules
    """
    def __init__(self, name):
        self.name = name
        self._setup_done = False
        self._teardown = False
        self._maxMessage = 10

    def makeTables(self, component):
        """ Defines the database tables for a component, such as "WMCore.ThreadPool" """
        myThread.transaction = Transaction(myThread.dbi)
        myThread.transaction.begin()
        factory = WMFactory(factoryName, factoryName + "." + \
            myThread.dialect)
        create = factory.loadObject("Create")
        createworked = create.execute(conn = myThread.transaction.conn)
        if createworked:
            logging.debug("Tables for "+ factoryName + " created")
        else:
            logging.debug("Tables " + factoryName + \
            " could not be created, already exists?")
        myThread.transaction.commit()


    def setUp(self):
        """
        setup for test.
        """
        if not self._setup_done:
            logging.basicConfig(level=logging.NOTSET,
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M',
                filename='%s.log' % __file__,
                filemode='w')

            myThread = threading.currentThread()
            myThread.logger = logging.getLogger(self.name)
            myThread.dialect = 'MySQL'

            options = {}
            options['unix_socket'] = os.getenv("DBSOCK")
            dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                options)

            myThread.dbi = dbFactory.connect()
            self._setup_done = True

    def msgService(self):
        """ returns a MsgService instance """
        # load a message service as we want to check if total failure
        # messages are returned
        if not self._setup_done:
            self.setUp()
        myThread = threading.currentThread()
        myThread.transaction = Transaction(myThread.dbi)
        myThread.transaction.begin()
        factory = WMFactory('msgService', 'WMCore.MsgService.'+myThread.dialect)
        msgService =  factory.loadObject("MsgService")
        msgService.registerAs(self.name)
        # subscribe to what we want to test.
        myThread.transaction.commit()
        return msgService

    def tearDown(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()
        if self._teardown and myThread.dialect == 'MySQL':
            # call the script we use for cleaning:
            command = os.getenv('WMCOREBASE')+ '/standards/./cleanup_mysql.sh'
            result = commands.getstatusoutput(command)
            for entry in result:
                print(str(entry))

        self._teardown = False


    def testA(self):
        """
        Mimics creation of component and handles come messages.
        """
        # read the default config first.
        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), \
            'src/python/WMComponent/ErrorHandler/DefaultConfig.py'))

        # we set the maxRetries to 10 for testing purposes
        config.ErrorHandler.maxRetries = 10
        # some general settings that would come from the general default 
        # config file
        config.Agent.contact = "fvlingen@caltech.edu"
        config.Agent.teamName = "Lakers"
        config.Agent.agentName = "Lebron James"

        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR")

       
        config.section_("CoreDatabase")
        config.CoreDatabase.dialect = 'mysql' 
        config.CoreDatabase.socket = os.getenv("DBSOCK")
        config.CoreDatabase.user = os.getenv("DBUSER")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        config.CoreDatabase.hostname = os.getenv("DBHOST")
        config.CoreDatabase.name = os.getenv("DBNAME")


