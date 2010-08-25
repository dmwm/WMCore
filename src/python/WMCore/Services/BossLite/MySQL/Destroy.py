#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.1 2009/07/09 18:47:44 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION

from WMCore.JobStateMachine.MySQL.Create import Create

class Destroy(Create):    
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
            
        Create.__init__(self, logger, dbi)

        self.create = {}

        self.create['01jt_group']        = "DROP TABLE jt_group"
        self.create['02bl_runningjob']   = "DROP TABLE bl_runningjob"
        self.create['03bl_job']          = "DROP TABLE bl_job"
        self.create['04bl_task']         = "DROP TABLE bl_task"

        self.requiredTables = []

        return
    
