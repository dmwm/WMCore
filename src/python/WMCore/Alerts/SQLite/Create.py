#!/usr/bin/python
"""
_Create_

Create the "alert_current" and "alert_history" tables for the alert
system in a MySQL database.
"""

__revision__ = "$Id: Create.py,v 1.1 2009/07/10 21:45:34 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_

    Create the "alert_current" and "alert_history" tables for the alert
    system in a MySQL database.
    """
    def __init__(self):
        """
        ___init___

        Create the "alert_current" and "alert_history" tables, nothing else is
        needed.
        """
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}

        self.create["alert_current"] = \
          """CREATE TABLE alert_current (
             id        INTEGER      PRIMARY KEY AUTOINCREMENT,
             severity  VARCHAR(30)  NOT NULL,
             component VARCHAR(30)  NOT NULL,
             message   VARCHAR(900) NOT NULL,
             time      INTEGER      NOT NULL
             )"""

        self.create["alert_history"] = \
          """CREATE TABLE alert_history (
             id             INTEGER      PRIMARY KEY AUTOINCREMENT,
             severity       VARCHAR(30)  NOT NULL,
             component      VARCHAR(30)  NOT NULL,
             message        VARCHAR(900) NOT NULL,
             generationtime INTEGER      NOT NULL,
             historytime    INTEGER      NOT NULL
             )"""
