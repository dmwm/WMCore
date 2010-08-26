#!/usr/bin/env python
"""
_WMLogging

Additional log levels used within wmcore.

"""
__all__ = []
__revision__ = "$Id: WMLogging.py,v 1.1 2008/10/02 14:31:38 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"


import logging

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

