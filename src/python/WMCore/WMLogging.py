#!/usr/bin/env python
"""
_WMLogging_

Logging facilities used in WMCore.
"""

import logging
from logging.handlers import HTTPHandler, RotatingFileHandler, TimedRotatingFileHandler

# a new log level which is lower than debug
# to prevent a tsunami of log messages in debug
# mode but to have the possibility to see all
# database queries if necessary.
logging.SQLDEBUG = 5
logging.addLevelName(logging.SQLDEBUG,"SQLDEBUG")

def sqldebug(msg):
    """
    A convenience method that all default levels
    have for publishing log messages.
    """
    logging.log(logging.SQLDEBUG, msg)

def setupRotatingHandler(fileName, maxBytes = 200000000, backupCount = 3):
    """
    _setupRotatingHandler_

    Create a rotating log handler with the given parameters.
    """
    handler = RotatingFileHandler(fileName, "a", maxBytes, backupCount)
    logging.getLogger().addHandler(handler)
    return


def getTimeRotatingLogger(name, logFile, duration = 'midnight'):
    """ Set the logger for time based lotaing. 
    """
    logger = logging.getLogger(name)
    handler = TimedRotatingFileHandler(logFile, duration, backupCount = 10)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    return logger

class CouchHandler(logging.handlers.HTTPHandler):
    def __init__(self, host, database):
        HTTPHandler.__init__(self, host, database, 'POST')
        from WMCore.Database.CMSCouch import CouchServer
        self.database = CouchServer(dburl=host).connectDatabase(database, size=10)

    def emit(self, record):
        """
        Write a document to CouchDB representing the log message.
        """
        doc = {}
        doc['message'] = record.msg
        doc['threadName'] = record.threadName
        doc['name'] = record.name
        doc['created'] = record.created
        doc['process'] = record.process
        doc['levelno'] = record.levelno
        doc['lineno'] = record.lineno
        doc['processName'] = record.processName
        doc['levelname'] = record.levelname
        self.database.commitOne(doc, timestamp=True)
