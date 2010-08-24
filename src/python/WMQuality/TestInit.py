#!/usr/bin/env python
"""
_TestInit__

A set of initialization steps used in many tests.
Test can call the methods from this class to 
initialize their default environment so to 
minimize code duplication.

This class is not a test but an auxilary class.

"""
__revision__ = \
    "$Id: TestInit.py,v 1.1 2008/11/12 16:15:02 fvlingen Exp $"
__version__ = \
    "$Revision: 1.1 $"
__author__ = \
    "fvlingen@caltech.edu"

import commands
import logging
import os
import threading

from WMCore.Agent.Configuration import Configuration
from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory


class TestInit:
    """
    A set of initialization steps used in many tests.
    Test can call the methods from this class to 
    initialize their default environment so to 
    minimize code duplication.
    """ 

    def __init__(self, testClassName, backend = 'MySQL'):
        self.testClassName = testClassName
        self.backend = backend

    def setLogging(self):
        """
        Sets logging parameters
        """
        logging.basicConfig(level=logging.DEBUG,\
            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',\
            datefmt='%m-%d %H:%M',\
            filename='%s.log' % self.testClassName,\
            filemode='w')
        logging.debug("Log file ready")
        myThread = threading.currentThread()
        myThread.logger = logging.getLogger(self.testClassName)


    def setDatabaseConnection(self):
        """
        Set up the database connection by retrieving the environment
        parameters.
        """

        myThread = threading.currentThread()
        myThread.logger = logging.getLogger('ErrorHandlerTest')
        myThread.dialect = self.backend

        options = {}
        if myThread.dialect == 'MySQL':
            options['unix_socket'] = os.getenv("DBSOCK")
            myThread.dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                options)
        else:
            myThread.dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"))

        myThread.dbi = myThread.dbFactory.connect()
        myThread.transaction = Transaction(myThread.dbi)
        myThread.transaction.commit()

    def setSchema(self, customModules = [], useDefault = True):
        """
        Creates the schema in the database for the default 
        tables/services: trigger, message service, threadpool.
       
        Developers can add their own modules to it using the array
        customModules which should follow the proper naming convention.

        if useDefault is set to False, it will not instantiate the
        schemas in the defaultModules array.
        """
        myThread = threading.currentThread()

        defaultModules = ["WMCore.MsgService", "WMCore.ThreadPool", \
            "WMCore.Trigger"]
        if not useDefault:
            defaultModules = []

        # filter out unique modules
        modules = {}
        for module in (defaultModules + customModules):
            modules[module] = 'done'

        myThread.transaction.begin()
        for factoryName in modules.keys():
            # need to create these tables for testing.
            # notice the default structure: <dialect>/Create
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

    def getConfiguration(self, configurationFile = None):
        """ 
        Loads (if available) your configuration file and augments
        it with the standard settings used in multiple tests.
        """
        if configurationFile != None:
            config = loadConfigurationFile(configurationFile)
        else:
            config = Configuration()

        # some general settings that would come from the general default
        # config file
        config.Agent.contact = "fvlingen@caltech.edu"
        config.Agent.teamName = "Lakers"
        config.Agent.agentName = "Lebron James"

        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR")

        config.section_("CoreDatabase")
        myThread = threading.currentThread()
        if self.backend == 'MySQL':
            config.CoreDatabase.dialect = 'mysql'
        if self.backend == 'Oracle':
            config.CoreDatabase.dialect = 'oracle'
        config.CoreDatabase.socket = os.getenv("DBSOCK")
        config.CoreDatabase.user = os.getenv("DBUSER")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        config.CoreDatabase.hostname = os.getenv("DBHOST")
        config.CoreDatabase.name = os.getenv("DBNAME")

        # after this you can augment it with whatever you need.
        return config

    def clearDatabase(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()
        # need to find a way to do this for oracle dbs too.
        if myThread.dialect == 'MySQL':
            # call the script we use for cleaning:
            command = os.getenv('WMCOREBASE')+ '/standards/./cleanup_mysql.sh'
            result = commands.getstatusoutput(command)
            for entry in result:
                print(str(entry))




