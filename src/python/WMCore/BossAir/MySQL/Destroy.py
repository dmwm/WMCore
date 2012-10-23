#/usr/bin/env python2.4
"""
_Destroy_

MySQL implementation of BossAir.Destroy
"""

__revision__ = "$Id: Destroy.py,v 1.2 2010/05/10 13:34:33 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION

class Destroy(DBCreator):
    """
    BossAir.Destroy
    """

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


        self.create['01bl_runjob']    = "DROP TABLE bl_runjob"
        self.create['02bl_status']    = "DROP TABLE bl_status"

        self.requiredTables = []

        return
