#/usr/bin/env python2.4
"""
_Destroy_

"""




import threading
import string

from WMCore.Database.DBCreator import DBCreator
from WMCore.RequestManager.RequestDB.MySQL.Create import Create

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
        orderedTables = Create.requiredTables[:]
        orderedTables.reverse()
        i = 0
        for tableName in orderedTables:
            i += 1
            prefix = string.zfill(i, 2)
            self.create[prefix + tableName] = "DROP TABLE %s" % tableName
