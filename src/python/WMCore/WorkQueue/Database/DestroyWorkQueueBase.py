#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: DestroyWorkQueueBase.py,v 1.3 2009/08/18 23:18:17 swakef Exp $"
__version__ = "$Revision: 1.3 $"

import threading
import string

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION
from CreateWorkQueueBase import CreateWorkQueueBase

class DestroyWorkQueueBase(DBCreator):    
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
        orderedTables = CreateWorkQueueBase.requiredTables[:]
        orderedTables.reverse()
        i = 0
        for requiredTable in orderedTables:
            i += 1
            tableName = requiredTable[2:]
            prefix = string.zfill(i, 2)
            self.create[prefix + tableName] = "DROP TABLE %s" % tableName