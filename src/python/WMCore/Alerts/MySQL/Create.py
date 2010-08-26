#!/usr/bin/python
"""
_Create_

Create the "alert_current" and "alert_history" tables for the alert
system in a MySQL database.
"""

__revision__ = "$Id: Create.py,v 1.4 2009/10/27 18:57:19 sryu Exp $"
__version__ = "$Revision: 1.4 $"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_

    Create the "alert_current" and "alert_history" tables for the alert
    system in a MySQL database.
    """
    def __init__(self, logger = None, dbi = None):
        """
        ___init___

        Create the "alert_current" and "alert_history" tables, nothing else is
        needed.
        """
        myThread = threading.currentThread()
        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi
        DBCreator.__init__(self, logger, dbi)
        self.create = {}
        self.constraints = {}

        self.create["alert_current"] = \
          """CREATE TABLE alert_current (
             id        INTEGER      NOT NULL AUTO_INCREMENT,
             severity  VARCHAR(30)  NOT NULL,
             component VARCHAR(30)  NOT NULL,
             message   VARCHAR(4000) NOT NULL,
             time      INTEGER      NOT NULL,
             PRIMARY KEY (id))"""

        self.create["alert_history"] = \
          """CREATE TABLE alert_history (
             id             INTEGER      NOT NULL AUTO_INCREMENT,
             severity       VARCHAR(30)  NOT NULL,
             component      VARCHAR(30)  NOT NULL,
             message        VARCHAR(4000) NOT NULL,
             generationtime INTEGER      NOT NULL,
             historytime    INTEGER      NOT NULL,
             PRIMARY KEY (id))"""
