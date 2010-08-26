#/usr/bin/env python2.4
"""
_DestroyAgentBase_

"""

__revision__ = "$Id: DestroyAgentBase.py,v 1.1 2010/06/21 21:19:27 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import threading
import string

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION
from CreateAgentBase import CreateAgentBase

class DestroyAgentBase(DBCreator):    
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
        orderedTables = CreateAgentBase.requiredTables[:]
        orderedTables.reverse()
        i = 0
        for requiredTable in orderedTables:
            i += 1
            tableName = requiredTable[2:]
            prefix = string.zfill(i, 2)
            self.create[prefix + tableName] = "DROP TABLE %s" % tableName