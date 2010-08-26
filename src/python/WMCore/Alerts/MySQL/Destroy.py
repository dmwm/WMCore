#!/usr/bin/python
"""
_Destroy_

Remove the alert_current and alert_history tables.
"""

__revision__ = "$Id: Destroy.py,v 1.2 2009/08/12 19:55:23 sryu Exp $"
__version__ = "$Revision: 1.2 $"

import threading

from WMCore.Database.DBCreator import DBCreator

class Destroy(DBCreator):
    """
    _Destroy_

    Remove the alert_current and alert_history tables.
    """
    def __init__(self, logger = None, dbi = None):
        """
        ___init___

        Drop the "alert_current" and "alert_history" tables, nothing else is
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

        self.create["alert_history"] = "DROP TABLE alert_history"
        self.create["alert_current"] = "DROP TABLE alert_current"
