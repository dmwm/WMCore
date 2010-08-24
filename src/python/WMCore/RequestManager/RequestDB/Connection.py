#!/usr/bin/env python
"""
_Connection_

get a database connection and DAOFactory instance for the RequestDB package

"""

import logging
#CHANGED FVL (1 line)
import threading

from WMCore.DAOFactory import DAOFactory
from WMQuality.TestInit import TestInit
from WMCore.WMFactory import WMFactory


def initReqDB():
    # setup connection
    testInit = TestInit('RequestDBConnection')
    testInit.setLogging(logLevel = logging.DEBUG)
    testInit.setDatabaseConnection()

def getConnection():
    """
    _getConnection_

    Get a connection to the DB and return a factory object to build
    query objects

    """
    myThread = threading.currentThread()
    factory = DAOFactory(package = 'WMCore.RequestManager.RequestDB',
                         logger = logging.getLogger(),
                         dbinterface = myThread.dbi)
    return factory
