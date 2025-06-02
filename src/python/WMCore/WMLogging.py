#!/usr/bin/env python
"""
_WMLogging_

Logging facilities used in WMCore.
"""
from datetime import datetime, date
import logging
from logging.handlers import (HTTPHandler,
                              RotatingFileHandler,
                              TimedRotatingFileHandler)
from pathlib import Path


# a new log level which is lower than debug
# to prevent a tsunami of log messages in debug
# mode but to have the possibility to see all
# database queries if necessary.
logging.SQLDEBUG = 5
logging.addLevelName(logging.SQLDEBUG, "SQLDEBUG")


def sqldebug(msg):
    """
    A convenience method that all default levels
    have for publishing log messages.
    """
    logging.log(logging.SQLDEBUG, msg)


def setupRotatingHandler(fileName, maxBytes=200000000, backupCount=3):
    """
    _setupRotatingHandler_

    Create a rotating log handler with the given parameters.
    """
    handler = RotatingFileHandler(fileName, "a", maxBytes, backupCount)
    logging.getLogger().addHandler(handler)


def getTimeRotatingLogger(name, logFile, duration='midnight'):
    """
    Set the logger for time based rotating.
    """
    logger = logging.getLogger(name)
    if duration == 'midnight':
        handler = WMTimedRotatingFileHandler(logFile, when=duration, backupCount=10)
    else:
        handler = TimedRotatingFileHandler(logFile, when=duration, backupCount=10)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    return logger


class WMTimedRotatingFileHandler(TimedRotatingFileHandler):
    """
    _WMTimedRotatingFileHandler_

    Overwrite the standard filename functionality from
    logging.handlers.TimedRotatingFileHandler
    such that it mimics the same behaviour as rotatelogs tool.

    More information here:
    https://docs.python.org/3/library/logging.handlers.html#baserotatinghandler
    """
    def __init__(self, filename, when='midnight', backupCount=10, **kwargs):
        """
        Initializes WMTimedRotatingFileHandler
        """
        super().__init__(filename, when=when, backupCount=backupCount,
                         delay=True, **kwargs)

        today = datetime.now().strftime(self.suffix).replace('-', '')
        # store our filename and set the filename to be used
        self.loggerBaseName = self.baseFilename
        filepath = Path(self.baseFilename)
        self.baseFilename = f"{filepath.parent}/{filepath.stem}-{today}{filepath.suffix}"

        self.stream = self._open()

    def namer(self, default_name):
        """
        namer called by rotation_filename in TimedRotatingFileHandler.doRollover

        For us to replicate rotatelogs, we need to add the time_str to the
        filename before the .log suffix
        """
        # get the time from default_name
        today = datetime.now().strftime(self.suffix).replace('-', '')
        time_str = today.replace('-', '')
        log_path = Path(self.loggerBaseName)
        logPath = f"{log_path.parent}/{log_path.stem}-{time_str}{log_path.suffix}"
        return logPath

    def rotator(self, src, dest):
        """
        rotator is called by the self.rotate

        Gets the new date, then sets the proper self.baseFilename
        We don't use the default behavior of the current file being renamed
        """
        today = datetime.now().strftime(self.suffix).replace('-', '')
        filepath = Path(self.loggerBaseName)
        self.baseFilename = f"{filepath.parent}/{filepath.stem}-{today}{filepath.suffix}"


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
