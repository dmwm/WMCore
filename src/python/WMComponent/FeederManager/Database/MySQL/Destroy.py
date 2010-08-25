#/usr/bin/env python
"""
_Destroy_

Implementation of FeederManager.Destroy for MySQL
"""

__revision__ = "$Id: Destroy.py,v 1.1 2009/11/06 23:50:57 riahi Exp $"
__version__ = "$Revision: 1.1 $s"

import threading

from WMCore.Database.DBCreator import DBCreator

class Destroy(DBCreator):
    """
    Class for destroying MySQL specific tables for the FeederManager
    """

    def __init__(self):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.delete["02managed_feeders"] = "DROP TABLE managed_feeders"
        self.delete["01managed_filesets"] = "DROP TABLE managed_filesets"
