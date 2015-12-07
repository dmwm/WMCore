#/usr/bin/env python
"""
_Destroy_

Clear out the ResourceControl schema.
"""




import threading
from WMCore.Database.DBCreator import DBCreator

class Destroy(DBCreator):
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the DBCreator constructor and create the list of required tables.
        """
        myThread = threading.currentThread()

        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi

        DBCreator.__init__(self, logger, dbi)

        self.create = {}
        self.delete = {}
        self.delete["rc_threshold"] = "DROP TABLE rc_threshold"

        return
