#!/usr/bin/env python
"""
_Connection_

get a database connection and DAOFactory instance for the RequestDB package

"""

import logging
import threading

from WMCore.DAOFactory import DAOFactory


def getConnection():
    """
    _getConnection_

    Get a connection to the DB and return a factory object to build
    query objects

    """
    myThread = threading.currentThread()
    factory = DAOFactory(package='WMCore.RequestManager.RequestDB',
                         logger=logging.getLogger(),
                         dbinterface=myThread.dbi)
    return factory
