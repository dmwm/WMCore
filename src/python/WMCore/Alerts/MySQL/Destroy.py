#!/usr/bin/python
"""
_Destroy_

Remove the alert_current and alert_history tables.
"""

__revision__ = "$Id: Destroy.py,v 1.1 2008/10/22 21:31:09 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBCreator import DBCreator

class Destroy(DBCreator):
    """
    _Destroy_

    Remove the alert_current and alert_history tables.
    """
    def __init__(self):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}

        self.create["alert_history"] = "DROP TABLE alert_history"
        self.create["alert_current"] = "DROP TABLE alert_current"
