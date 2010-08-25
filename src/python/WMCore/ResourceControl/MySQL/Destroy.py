#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.1 2009/10/05 20:03:00 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

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
        self.delete["03rc_site"]             = "DROP TABLE rc_site"
        self.delete["02rc_site_threshold"]   = "DROP TABLE rc_site_threshold"
        self.delete["01rc_site_attr"]        = "DROP TABLE rc_site_attr"
