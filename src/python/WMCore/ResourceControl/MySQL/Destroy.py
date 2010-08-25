#/usr/bin/env python
"""
_Destroy_

Clear out the ResourceControl schema.
"""

__revision__ = "$Id: Destroy.py,v 1.2 2010/02/09 17:59:14 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

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
        self.delete["01rc_thresholds"] = "DROP TABLE rc_threshold"

        return
