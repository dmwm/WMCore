#!/usr/bin/python
"""
_WMInit

Init class that can be used by external projects
that only use part of the libraries
"""





import logging
import threading

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory
from WMCore.Configuration import loadConfigurationFile

import os.path
import sys
def getWMBASE():
    """ returns the root of WMCore install """
    return os.path.normpath( os.path.join(os.path.dirname(__file__), '..', '..','..' ) )

def connectToDB():
    """
    _connectToDB_

    Connect to the database specified in the WMAgent config.
    """
    if not os.environ.has_key("WMAGENT_CONFIG"):
        print "Please set WMAGENT_CONFIG to point at your WMAgent configuration."
        sys.exit(1)
        
    if not os.path.exists(os.environ["WMAGENT_CONFIG"]):
        print "Can't find config: %s" % os.environ["WMAGENT_CONFIG"]
        sys.exit(1)
        
    wmAgentConfig = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])
    
    if not hasattr(wmAgentConfig, "CoreDatabase"):
        print "Your config is missing the CoreDatabase section."
        sys.exit(1)
        
    socketLoc = getattr(wmAgentConfig.CoreDatabase, "socket", None)
    connectUrl = getattr(wmAgentConfig.CoreDatabase, "connectUrl", None)
    (dialect, junk) = connectUrl.split(":", 1)
    
    myWMInit = WMInit()
    myWMInit.setDatabaseConnection(dbConfig = connectUrl, dialect = dialect,
                                   socketLoc = socketLoc)
    return

class WMInit:

    def __init__(self):
        # pass for the moment
        pass
    
    def getWMBASE(self):
        """ for those that don't want to use the static version"""
        # I know that I am in src/python/WMCore/WMInit.py
        return os.path.normpath( os.path.join(os.path.dirname(__file__), '..', '..','..' ) )

    def setLogging(self,logFile = None, logName = None, logLevel = logging.INFO, logExists = True):
        """
        Sets logging parameters, depending on the settings,
        this method will create a logging file.
        """
        # use logName as name for file is no log file is given
        if not logExists:
            logging.basicConfig(level=logLevel,\
            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',\
            datefmt='%m-%d %H:%M',\
            filename='%s.log' % logFile,\
            filemode='w')
            logging.debug("Log file ready")

        myThread = threading.currentThread()
        if logName != None:
            myThread.logger = logging.getLogger(logName)
        else:
            myThread.logger = logging.getLogger()


    def setDatabaseConnection(self, dbConfig, dialect = None, socketLoc = None):
        """
        Sets the default connection parameters, without having to worry
        much on what attributes need to be set. This is esepcially 
        advantagous for developers of third party projects that want
        to use only parts of the WMCore lib.

        The class differentiates between different formats used by external 
        projects. External project formats that are supported can activated 
        it by setting the flavor flag.
        """
        myThread = threading.currentThread()
        if hasattr(myThread, "dialect"):
            if myThread.dialect != None and myThread.dialect != "SQLite":
                # Database is already initialized, we'll create a new
                # transaction and move on.
                if hasattr(myThread, "transaction"):
                    if myThread.transaction != None:
                        myThread.transaction.commit()

                myThread.transaction = Transaction(myThread.dbi)
                return

        options = {}            
        if dialect.lower() == 'mysql':
            dialect = 'MySQL'
            if socketLoc != None:
                options['unix_socket'] = socketLoc            
        elif dialect.lower() == 'oracle':
            dialect = 'Oracle'
        elif dialect.lower() == 'sqlite':
            dialect = 'SQLite'
        
        myThread.dialect = dialect
        myThread.logger = logging
        myThread.dbFactory = DBFactory(logging, dbConfig, options)
        myThread.dbi = myThread.dbFactory.connect()

        # The transaction object will begin a transaction as soon as it is
        # initialized.  I'd rather have the user handle that, so we'll commit
        # it here.
        myThread.transaction = Transaction(myThread.dbi)
        myThread.transaction.commit()                
        return

    def setSchema(self, modules = [], params = None):
        """
        Creates the schema in the database based on the modules
        input.

        This method needs to have been preceded by the 
        setDatabaseConnection.
        """
        myThread = threading.currentThread()

        parameters = None
        flag = False
        #Set up for typical DBCreator format: logger, dbi, params
        if params != None:
            parameters = [None, None, params]
            flag = True
        

        # filter out unique modules

        myThread.transaction.begin()
        for factoryName in modules:
            # need to create these tables for testing.
            # notice the default structure: <dialect>/Create
            factory = WMFactory(factoryName, factoryName + "." + \
                myThread.dialect)

            create = factory.loadObject("Create", args = parameters, listFlag = flag)
            createworked = create.execute(conn = myThread.transaction.conn,
                                          transaction = myThread.transaction)
            if createworked:
                logging.debug("Tables for "+ factoryName + " created")
            else:
                logging.debug("Tables " + factoryName + \
                " could not be created, already exists?")
        myThread.transaction.commit()

    def initializeSchema(self, nameSpace, modules = []):
        """
        Sometimes you need to initialize the schema before 
        starting the program. This methods lets you pass
        modules that have an execute method which contains
        arbitrary sql statements.
        """
        myThread = threading.currentThread()

        # filter out unique modules

        myThread.transaction.begin()

        factory = WMFactory(nameSpace, nameSpace + "." + myThread.dialect)

        for factoryName in modules:
            # need to create these tables for testing.
            # notice the default structure: <dialect>/Create
            create = factory.loadObject(factoryName)
            create.logger = logging
            create.dbi = myThread.dbi
            createworked = create.execute(conn = myThread.transaction.conn)
            if createworked:
                logging.debug("SQL for "+ factoryName + " executed")
            else:
                logging.debug("SQL " + factoryName + \
                " could not be executed, already exists?")
        myThread.transaction.commit()

    def clearDatabase(self, modules = []):
        """
        Enables clearing particular tables in the database
        Associated to modules. Note this only works if there 
        is the module has a Destroy class. Beware that this 
        might not work if there are table dependencies.
        """
        myThread = threading.currentThread()
        for module in modules:
            factory = WMFactory("clear", module)
            destroy = factory.loadObject(myThread.dialect+".Destroy")
            myThread.transaction.begin()
            destroyworked = destroy.execute(conn = myThread.transaction.conn)
            if not destroyworked:
                raise Exception(module  +" tables could not be destroyed")
            myThread.transaction.commit()
            del factory
 































