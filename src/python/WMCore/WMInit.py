#!/usr/bin/env python
"""
_WMInit

Init class that can be used by external projects
that only use part of the libraries
"""
from __future__ import print_function

import logging
import os
import os.path
import sys
import threading
import traceback

from WMCore.Configuration import loadConfigurationFile
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMBase import getWMBASE
from WMCore.WMException import WMException
from WMCore.WMFactory import WMFactory


class WMInitException(WMException):
    """
    WMInitException

    You should never, ever see one of these.
    I'm not optimistic that this will be the case.
    """


def connectToDB():
    """
    _connectToDB_

    Connect to the database specified in the WMAgent config.
    """
    if "WMAGENT_CONFIG" not in os.environ:
        print("Please set WMAGENT_CONFIG to point at your WMAgent configuration.")
        sys.exit(1)

    if not os.path.exists(os.environ["WMAGENT_CONFIG"]):
        print("Can't find config: %s" % os.environ["WMAGENT_CONFIG"])
        sys.exit(1)

    wmAgentConfig = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    if not hasattr(wmAgentConfig, "CoreDatabase"):
        print("Your config is missing the CoreDatabase section.")
        sys.exit(1)

    socketLoc = getattr(wmAgentConfig.CoreDatabase, "socket", None)
    connectUrl = getattr(wmAgentConfig.CoreDatabase, "connectUrl", None)
    (dialect, junk) = connectUrl.split(":", 1)

    myWMInit = WMInit()
    myWMInit.setDatabaseConnection(dbConfig=connectUrl, dialect=dialect,
                                   socketLoc=socketLoc)
    return


class WMInit(object):
    def __init__(self):
        return

    def getWMBASE(self):
        """ for those that don't want to use the static version"""
        return getWMBASE()

    def setLogging(self, logFile=None, logName=None, logLevel=logging.INFO, logExists=True):
        """
        Sets logging parameters, depending on the settings,
        this method will create a logging file.
        """
        # use logName as name for file is no log file is given
        if not logExists:
            logging.basicConfig(level=logLevel, \
                                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', \
                                datefmt='%m-%d %H:%M', \
                                filename='%s.log' % logFile, \
                                filemode='w')
            logging.debug("Log file ready")

        myThread = threading.currentThread()
        if logName != None:
            myThread.logger = logging.getLogger(logName)
        else:
            myThread.logger = logging.getLogger()

    def setDatabaseConnection(self, dbConfig, dialect, socketLoc=None):
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
        if getattr(myThread, "dialect", None) != None:
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
        elif dialect.lower() == 'http':
            dialect = 'CouchDB'
        else:
            msg = "Unsupported dialect %s !" % dialect
            logging.error(msg)
            raise WMInitException(msg)

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

    def setSchema(self, modules=None, params=None):
        """
        Creates the schema in the database based on the modules
        input.

        This method needs to have been preceded by the
        setDatabaseConnection.
        """
        modules = modules or []
        myThread = threading.currentThread()

        parameters = None
        flag = False
        # Set up for typical DBCreator format: logger, dbi, params
        if params != None:
            parameters = [None, None, params]
            flag = True

        myThread.transaction.begin()
        for factoryName in modules:
            # need to create these tables for testing.
            # notice the default structure: <dialect>/Create
            factory = WMFactory(factoryName, factoryName + "." + myThread.dialect)

            create = factory.loadObject("Create", args=parameters, listFlag=flag)
            createworked = create.execute(conn=myThread.transaction.conn,
                                          transaction=myThread.transaction)
            if createworked:
                logging.debug("Tables for " + factoryName + " created")
            else:
                logging.debug("Tables " + factoryName + " could not be created.")
        myThread.transaction.commit()

    def clearDatabase(self, modules=None):
        """
        Database deletion. Global, ignore modules.
        """
        myThread = threading.currentThread()
        if hasattr(myThread, 'transaction') and getattr(myThread.transaction, 'transaction', None):
            # Then we have an open transaction
            # We should try and close it first
            try:
                myThread.transaction.commit()
            except:
                try:
                    myThread.transaction.rollback()
                except:
                    pass

        # Setup the DAO
        daoFactory = DAOFactory(package="WMCore.Database",
                                logger=myThread.logger,
                                dbinterface=myThread.dbi)
        destroyDAO = daoFactory(classname="Destroy")

        # Actually run a transaction and delete the DB
        try:
            destroyDAO.execute()
        except Exception as ex:
            msg = "Critical error while attempting to delete entire DB!\n"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            raise WMInitException(msg)

        return

    def checkDatabaseContents(self):
        """
        _checkDatabaseContents_

        Check and see if anything is in the database.
        This should be called by methods about to build the schema to make sure
        that the DB itself is empty.
        """

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.Database",
                                logger=myThread.logger,
                                dbinterface=myThread.dbi)

        testDAO = daoFactory(classname="ListUserContent")

        result = testDAO.execute()
        myThread.dbi.engine.dispose()

        return result
