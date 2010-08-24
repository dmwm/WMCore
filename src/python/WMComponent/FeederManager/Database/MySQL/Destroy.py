#/usr/bin/env python
"""
_Destroy_

Implementation of FeederManager.Destroy for MySQL
"""




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
