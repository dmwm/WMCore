#/usr/bin/env python2.4
"""
_Destroy_

"""




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
        self.delete = {}
        self.delete["03tp_threadpool"]             = "DROP TABLE tp_threadpool"
        self.delete["02tp_threadpool_buffer_in"]   = "DROP TABLE tp_threadpool_buffer_in"
        self.delete["01tp_threadpool_buffer_out"]  = "DROP TABLE tp_threadpool_buffer_out"
