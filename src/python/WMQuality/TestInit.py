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
    "$Id: TestInit.py,v 1.48 2010/02/09 20:10:18 meloam Exp $"
__version__ = \
    "$Revision: 1.48 $"
__author__ = \
    "fvlingen@caltech.edu"

import commands
import logging
import os
import threading
import tempfile
import shutil
import time

from WMCore.Agent.Configuration import Configuration
from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMInit import WMInit

# Sorry for the global, but I think this should go here
trashDatabases = False  # delete databases after every test?
def deleteDatabaseAfterEveryTest( areYouSerious ):
    """ this method handles whether or not TestInit will be vicious
        to databases
    """
    # python is idiotic for its scoping system
    global trashDatabases
    if (areYouSerious == "I'm Serious"):
        print "We are going to trash databases after every test"
        trashDatabases = True
    else:
        #"I'm glad you weren't serious"
        print "We are not going to trash databases after every test"
        trashDatabases = False
        
class TestInit:
    """
    A set of initialization steps used in many tests.
    Test can call the methods from this class to 
    initialize their default environment so to 
    minimize code duplication.
    """ 

    def __init__(self, testClassName):
        self.testClassName = testClassName
        self.testDir = None
        self.currModules = []
        self.init = WMInit()
        self.deleteTmp = True
    
    def __del__(self):
        if self.deleteTmp:
            self.delWorkDir()
        self.attemptToCloseDBConnections()

    
    def delWorkDir(self):
        if (self.testDir != None):
            try:
                shutil.rmtree( self.testDir )
            except:
                # meh, if it fails, I guess something weird happened
                pass

    def setLogging(self, logLevel = logging.INFO):
        """
        Sets logging parameters
        """
        # remove old logging instances.
        return
        logger1 = logging.getLogger()
        logger2 = logging.getLogger(self.testClassName)
        for logger in [logger1, logger2]:
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)

        self.init.setLogging(self.testClassName, self.testClassName,
                             logExists = False, logLevel = logLevel)
    
    def generateWorkDir(self, config = None, deleteOnDestruction = True):
        self.deleteTmp = deleteOnDestruction
        self.testDir = tempfile.mkdtemp()
        if config:
            config.section_("General")
            config.General.workDir = self.testDir
        os.environ['TESTDIR'] = self.testDir
        return self.testDir
        
    def getBackendFromDbURL(self, dburl):
        dialectPart = dburl.split(":")[0]
        if dialectPart == 'mysql':
            return 'MySQL'
        elif dialectPart == 'sqlite':
            return 'SQLite'
        elif dialectPart == 'oracle':
            return 'Oracle'
        else:
            raise RuntimeError, "Unrecognized dialect"
        
    def eraseEverythingInDatabase(self):
        if trashDatabases:
            dbi = self.getDBInterface()
            dialect = self.coreConfig.CoreDatabase.dialect
            formatter = DBFormatter(logging.getLogger(''), dbi)
            if (dialect == 'MySQL'):
                print "wiping mysql"
                formatter.sql = r"SHOW TABLES"
                result = formatter.execute()
                print "we got the following result for SHOW TABLES: %s " % result
                formatter.sql = "SET foreign_key_checks = 0"
                formatter.execute()
                tableNames = []
                if len(tableNames) == 0:
                    return
                
                for oneTable in result:
                    tableNames.append( oneTable[0] )
                tableList = ",".join( tableNames )
                query = "DROP TABLE IF EXISTS %s" % tableList
                print query
                formatter.sql = query
                formatter.execute()
                formatter.sql = "SET foreign_key_checks = 1"
                formatter.execute()
                
            elif (dialect == 'SQLite'):
                formatter.sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
                result = formatter.execute()

                for oneTable in result:
                    # sqlite stores some magic in the database
                    if ( oneTable[0].startswith('sqlite_') ):
                        continue
                    query = "DROP TABLE IF EXISTS %s" % oneTable[0]
                    failCount = 0
                    for x in range(5):
                        try:
                            formatter.sql = query
                            formatter.execute()
                        except Exception:
                            # sleep a sec and try again
                            failCount = failCount + 1
                            if (failCount == 5):
                                raise
                            else:
                                time.sleep(1)
                            
      
            elif (dialect == 'Oracle'):
                pass
            else:
                raise RuntimeError, "This dialect is unsupported by trashDatabases"
            pass
        else:
            pass
        
    def setDatabaseConnection(self, connectUrl=None, socket=None):
        """
        Set up the database connection by retrieving the environment
        parameters.
        """        
        config = self.getConfiguration(connectUrl=connectUrl, socket=socket)
        self.coreConfig = config
        self.init.setDatabaseConnection(
                                        config.CoreDatabase.connectUrl,
                                        config.CoreDatabase.dialect,
                                        config.CoreDatabase.socket)
        if trashDatabases:
            # we are going to own ths database.
            #  ...but the code isn't ready yet
            self.eraseEverythingInDatabase()
            pass
        

    def setSchema(self, customModules = [], useDefault = True, params = None):
        """
        Creates the schema in the database for the default 
        tables/services: trigger, message service, threadpool.
       
        Developers can add their own modules to it using the array
        customModules which should follow the proper naming convention.

        if useDefault is set to False, it will not instantiate the
        schemas in the defaultModules array.
        """
        defaultModules = ["WMCore.MsgService", "WMCore.ThreadPool", \
                          "WMCore.Trigger"]
        if not useDefault:
            defaultModules = []

        # filter out unique modules
        modules = {}
        for module in (defaultModules + customModules):
            modules[module] = 'done'
            
        try:
            self.init.setSchema(modules.keys(), params = params)
        except Exception, ex:
            self.clearDatabase(modules = modules.keys())
            raise
            
        # store the list of modules we've added to the DB
        modules = {}
        for module in (defaultModules + customModules + self.currModules):
            modules[module] = 'done'

        self.currModules = modules.keys()
        return

    def initializeSchema(self, modules = []):
        """
        Sometimes you need to initialize the schema before
        starting the program. This methods lets you pass
        modules that have an execute method which contains
        arbitrary sql statements.
        """
        self.init.initializeSchema(modules)
        
    def getDBInterface(self):
        "shouldbe called after connection is made"
        myThread = threading.currentThread()
        return myThread.dbi

    def getConfiguration(self, configurationFile = None, connectUrl = None, socket=None):
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
        # If you need a testDir, call testInit.generateWorkDir
        # config.General.workDir = os.getenv("TESTDIR")

        config.section_("CoreDatabase")
        if connectUrl:
            config.CoreDatabase.connectUrl = connectUrl
            config.CoreDatabase.dialect = self.getBackendFromDbURL(connectUrl)
            config.CoreDatabase.socket = socket or os.getenv("DBSOCK") 
        else:
            if (os.getenv('DATABASE') == None):
                raise RuntimeError, \
                    "You must set the DATABASE environment variable to run tests"
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")
            config.CoreDatabase.dialect = self.getBackendFromDbURL( os.getenv("DATABASE") )
            config.CoreDatabase.socket = os.getenv("DBSOCK")
            if os.getenv("DBHOST"):
                print "****WARNING: the DBHOST environment variable will be deprecated soon***"
                print "****WARNING: UPDATE YOUR ENVIRONMENT OR TESTS WILL FAIL****"
            # after this you can augment it with whatever you need.
        return config

    def clearDatabase(self, modules = []):
        """
        Database deletion. If no modules are specified
        it will clear the tables we added with setschema
        """
        if modules == []:
            modules = self.currModules

        modules.reverse()
        self.init.clearDatabase(modules)
        self.attemptToCloseDBConnections()


    def attemptToCloseDBConnections(self):
        myThread = threading.currentThread()

        try:
            myThread.dbi.close()
        except Exception, e:
            print "tried to close DBI %s" % e
        
        try:
            del myThread.dbFactory 
        except Exception, e:
            print "tried to delete factory %s" % e
        
        
