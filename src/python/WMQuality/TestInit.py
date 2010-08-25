#!/usr/bin/env python
"""
_TestInit__

A set of initialization steps used in many tests.
Test can call the methods from this class to 
initialize their default environment so to 
minimize code duplication.

This class is not a test but an auxilary class and 
is based on the WMCore.WMInit class.

"""
__revision__ = \
    "$Id: TestInit.py,v 1.15 2009/09/30 23:41:26 meloam Exp $"
__version__ = \
    "$Revision: 1.15 $"
__author__ = \
    "fvlingen@caltech.edu"

import commands
import logging
import os
import threading

from WMCore.Agent.Configuration import Configuration
from WMCore.Agent.Configuration import loadConfigurationFile

from WMCore.WMInit import WMInit

class TestInit:
    """
    A set of initialization steps used in many tests.
    Test can call the methods from this class to 
    initialize their default environment so to 
    minimize code duplication.
    """ 

    def __init__(self, testClassName, backend = None):
        self.testClassName = testClassName
        if ( backend == None ):
            if ( os.getenv('DIALECT') == None ):
                backend = 'MySQL'
            else:
                backend = os.getenv('DIALECT')
        self.backend = backend
        self.init = WMInit()

    def setLogging(self, logLevel = logging.INFO):
        """
        Sets logging parameters
        """
        # remove old logging instances.
        logger1 = logging.getLogger()
        logger2 = logging.getLogger(self.testClassName)
        for logger in [logger1, logger2]:
            for handler in logger.handlers:
                logger.removeHandler(handler)

        self.init.setLogging(self.testClassName, self.testClassName,
                             logExists = False, logLevel = logLevel)

    def setDatabaseConnection(self):
        """
        Set up the database connection by retrieving the environment
        parameters.
        """
        if (os.getenv('DATABASE') == None):
            raise RuntimeError, \
                "You must set the DATABASE environment variable to run tests"
        
        # need to make sure that the dialect matches the database
        if ( not os.getenv('DATABASE').lower().startswith( \
                    self.backend.lower())):
            raise RuntimeError, \
                "It appears that your database doesn't match your dialect config"
        
        self.init.setDatabaseConnection(os.getenv("DATABASE"), \
            self.backend, os.getenv("DBSOCK"))

    def setSchema(self, customModules = [], useDefault = True, params = None):
        """
        Creates the schema in the database for the default 
        tables/services: trigger, message service, threadpool.
       
        Developers can add their own modules to it using the array
        customModules which should follow the proper naming convention.

        if useDefault is set to False, it will not instantiate the
        schemas in the defaultModules array.
        """
        try:
            defaultModules = ["WMCore.MsgService", "WMCore.ThreadPool", \
                              "WMCore.Trigger"]
            if not useDefault:
                defaultModules = []

            # filter out unique modules
            modules = {}
            for module in (defaultModules + customModules):
                modules[module] = 'done'
            self.init.setSchema(modules.keys(), params = params)
        except:
            self.init.clearDatabase()
        

    def initializeSchema(self, modules = []):
        """
        Sometimes you need to initialize the schema before
        starting the program. This methods lets you pass
        modules that have an execute method which contains
        arbitrary sql statements.
        """
        self.init.initializeSchema(modules)

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
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.dialect = self.backend.lower()
        config.CoreDatabase.socket = os.getenv("DBSOCK")
        config.CoreDatabase.user = os.getenv("DBUSER")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        config.CoreDatabase.hostname = os.getenv("DBHOST")
        config.CoreDatabase.name = os.getenv("DBNAME")

        # after this you can augment it with whatever you need.
        return config

    def clearDatabase(self, modules = []):
        """
        Database deletion. If no modules are specified
        it will clear the whole database.
        """
        myThread = threading.currentThread()
        # need to find a way to do this for oracle dbs too.
        # FIXME
        # this next block has problems. is there a reason to use it over
        # having WMInit tear down the database?
#        if myThread.dialect == 'MySQL' and modules == []:
#            # call the script we use for cleaning:
#            # FIXME: need to be more general
#            if (os.getenv('WMCOREBASE') == None):
#                raise RuntimeError, "WMCOREBASE environment variable undefined"
#            
#            command = os.getenv('WMCOREBASE')+ '/standards/./cleanup_mysql.sh'
#            result = commands.getstatusoutput(command)
#            for entry in result:
#                print(str(entry))
#        else:
        self.init.clearDatabase(modules)




