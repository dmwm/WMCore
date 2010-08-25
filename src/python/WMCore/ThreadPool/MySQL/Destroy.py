#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.2 2009/07/20 17:35:13 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION

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

        self.create["03tp_threadpool"]             = "DROP TABLE tp_threadpool"
        self.create["02tp_threadpool_buffer_in"]   = "DROP TABLE tp_threadpool_buffer_in"
        self.create["01tp_threadpool_buffer_out"]  = "DROP TABLE tp_threadpool_buffer_out"

