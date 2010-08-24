#!/usr/bin/python
"""
_Create_

Create the "alert_current" and "alert_history" tables for the alert
system in a MySQL database.
"""

__revision__ = "$Id: Create.py,v 1.1 2008/10/22 21:31:09 sfoulkes Exp $"
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
             id        INT(11)     NOT NULL AUTO_INCREMENT,
             severity  VARCHAR(30) NOT NULL,
             component VARCHAR(30) NOT NULL,
             message   TEXT        NOT NULL,
             time      TIMESTAMP   DEFAULT NOW(),
             PRIMARY KEY (id))"""

        self.create["alert_history"] = \
          """CREATE TABLE alert_history (
             id             INT(11)     NOT NULL AUTO_INCREMENT,
             severity       VARCHAR(30) NOT NULL,
             component      VARCHAR(30) NOT NULL,
             message        TEXT        NOT NULL,
             generationtime TIMESTAMP   DEFAULT 0,
             historytime    TIMESTAMP   DEFAULT NOW(),
             PRIMARY KEY (id))"""
