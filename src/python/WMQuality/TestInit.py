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
from __future__ import print_function

from builtins import object

import logging
import os
import shutil
import tempfile
import threading
import traceback

from WMCore.Agent.Configuration import Configuration
from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.WMException import WMException

hasDatabase = True
try:
    from WMCore.WMInit import WMInit
except ImportError as ex:
    print(str(ex))
    print("NOTE: TestInit is being loaded without database support")
    hasDatabase = False

# Sorry for the global, but I think this should go here
trashDatabases = False  # delete databases after every test?

class TestInitException(WMException):
    """
    _TestInitException_

    You should see this only if something is wrong setting up the database connection
    Still, just in case...
    """

def deleteDatabaseAfterEveryTest(areYouSerious):
    """
    this method handles whether or not TestInit will be vicious to databases
    """
    # python is idiotic for its scoping system
    global trashDatabases
    if areYouSerious == "I'm Serious":
        print("We are going to trash databases after every test")
        trashDatabases = True
    else:
        #"I'm glad you weren't serious"
        print("We are not going to trash databases after every test")
        trashDatabases = False


class TestInit(object):
    """
    A set of initialization steps used in many tests.
    Test can call the methods from this class to
    initialize their default environment so to
    minimize code duplication.
    """

    def __init__(self, testClassName = "Unknown Class"):
        self.testClassName = testClassName
        self.testDir = None
        self.currModules = []
        global hasDatabase
        self.hasDatabase = hasDatabase
        if self.hasDatabase:
            self.init = WMInit()
        self.deleteTmp = True

    def __del__(self):
        if self.deleteTmp:
            self.delWorkDir()
        self.attemptToCloseDBConnections()


    def delWorkDir(self):
        if self.testDir != None:
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

    def generateWorkDir(self, config = None, deleteOnDestruction = True, setTmpDir = False):
        self.testDir = tempfile.mkdtemp()
        if config:
            config.section_("General")
            config.General.workDir = self.testDir
        os.environ['TESTDIR'] = self.testDir
        if os.getenv('WMCORE_KEEP_DIRECTORIES', False):
            deleteOnDestruction = True
            logging.info("Generated testDir - %s" % self.testDir)
        if setTmpDir:
            os.environ['TMPDIR'] = self.testDir

        self.deleteTmp = deleteOnDestruction
        return self.testDir

    def getBackendFromDbURL(self, dburl):
        dialectPart = dburl.split(":")[0]
        if dialectPart == 'mysql':
            return 'MySQL'
        elif dialectPart == 'oracle':
            return 'Oracle'
        elif dialectPart == 'http':
            return 'CouchDB'
        else:
            raise RuntimeError("Unrecognized dialect %s" % dialectPart)

    def setDatabaseConnection(self, connectUrl=None, socket=None, destroyAllDatabase = False):
        """
        Set up the database connection by retrieving the environment
        parameters.

        The destroyAllDatabase option is for testing ONLY.  Never flip that switch
        on in any other instance where you don't know what you're doing.
        """
        if not self.hasDatabase:
            return
        config = self.getConfiguration(connectUrl=connectUrl, socket=socket)
        self.coreConfig = config
        self.init.setDatabaseConnection(config.CoreDatabase.connectUrl,
                                        config.CoreDatabase.dialect,
                                        config.CoreDatabase.socket)

        if trashDatabases or destroyAllDatabase:
            self.clearDatabase()

        # Have to check whether or not database is empty
        # If the database is not empty when we go to set the schema, abort!
        try:
            result = self.init.checkDatabaseContents()
        except Exception as e:
            logging.debug("Error checking DB contents, assume DB does not exist")
            logging.debug(str(e))
            return
        if len(result) > 0:
            msg = "Database not empty, cannot set schema !\n"
            msg += str(result)
            logging.error(msg)
            raise TestInitException(msg)

        return

    def setSchema(self, customModules = [], useDefault = True, params = None):
        """
        Creates the schema in the database for the default
        tables/services: trigger, message service, threadpool.

        Developers can add their own modules to it using the array
        customModules which should follow the proper naming convention.

        if useDefault is set to False, it will not instantiate the
        schemas in the defaultModules array.
        """
        if not self.hasDatabase:
            return
        defaultModules = ["WMCore.WMBS"]
        if not useDefault:
            defaultModules = []

        # filter for unique modules
        modules = []
        for module in (defaultModules + customModules):
            if module not in modules:
                modules.append(module)

        try:
            self.init.setSchema(modules, params = params)
        except Exception as ex:
            print(traceback.format_exc())
            raise ex

        # store the list of modules we've added to the DB
        # again filter for unique modules
        modules = []
        for module in (defaultModules + customModules + self.currModules):
            if module not in modules:
                modules.append(module)
        self.currModules = modules

        return

    def getDBInterface(self):
        "shouldbe called after connection is made"
        if not self.hasDatabase:
            return
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
        config.Agent.hostName = "testhost.laker.world"

        config.section_("General")
        # If you need a testDir, call testInit.generateWorkDir
        # config.General.workDir = os.getenv("TESTDIR")
        config.General.ReqMgr2ServiceURL = "http://localhost/reqmgr2"

        config.section_("CoreDatabase")
        if connectUrl:
            config.CoreDatabase.connectUrl = connectUrl
            config.CoreDatabase.dialect = self.getBackendFromDbURL(connectUrl)
            config.CoreDatabase.socket = socket or os.getenv("DBSOCK")
        else:
            if os.getenv('DATABASE') == None:
                raise RuntimeError("You must set the DATABASE environment variable to run tests")
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")
            config.CoreDatabase.dialect = self.getBackendFromDbURL(os.getenv("DATABASE"))
            config.CoreDatabase.socket = os.getenv("DBSOCK")
            if os.getenv("DBHOST"):
                print("****WARNING: the DBHOST environment variable will be deprecated soon***")
                print("****WARNING: UPDATE YOUR ENVIRONMENT OR TESTS WILL FAIL****")
            # after this you can augment it with whatever you need.

        couchurl = os.getenv("COUCHURL")
        config.section_("ACDC")
        config.ACDC.couchurl = couchurl
        config.ACDC.database = "wmagent_acdc_t"

        config.component_("JobStateMachine")
        config.JobStateMachine.couchurl = couchurl
        config.JobStateMachine.couchDBName = "wmagent_job_test"
        config.JobStateMachine.jobSummaryDBName = "job_summary"
        config.JobStateMachine.summaryStatsDBName = "stat_summary_test"

        config.component_("JobAccountant")
        config.JobAccountant.pollInterval = 60
        config.JobAccountant.componentDir = os.getcwd()
        config.JobAccountant.logLevel = 'SQLDEBUG'

        config.component_("TaskArchiver")
        config.TaskArchiver.localWMStatsURL = "%s/%s" % (config.JobStateMachine.couchurl, config.JobStateMachine.jobSummaryDBName)

        return config

    def clearDatabase(self, modules = []):
        """
        Database deletion. Global, ignore modules.
        """
        if not self.hasDatabase:
            return

        self.init.clearDatabase()

        return

    def attemptToCloseDBConnections(self):
        return
        myThread = threading.currentThread()
        print("Closing DB")

        try:
            if not myThread.transaction \
                 and not myThread.transaction.conn \
                 and not myThread.transaction.conn.closed:

                myThread.transaction.conn.close()
                myThread.transaction.conn = None
                print("Connection Closed")
        except Exception as e:
            print("tried to close DBI but failed: %s" % e)

        try:
            if hasattr(myThread, "dbFactory"):
                del myThread.dbFactory
                print("dbFactory removed")
        except Exception as e:
            print("tried to delete factory but failed %s" % e)
