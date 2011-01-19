#!/usr/bin/env python
"""
_WMLogging_

Logging facilities used in WMCore.
"""

import logging
import logging.handlers

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
    handler = logging.handlers.RotatingFileHandler(fileName, "a", maxBytes, backupCount)
    logging.getLogger().addHandler(handler)
    return
