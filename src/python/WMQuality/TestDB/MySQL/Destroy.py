#!/usr/bin/env python
"""
_Destroy_

Tear down the MySQL version of the TestDB schema.
"""

__revision__ = "$Id: Destroy.py,v 1.1 2010/03/01 16:49:02 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBCreator import DBCreator

class Destroy(DBCreator):
    def __init__(self, logger = None, dbi = None, params = None):
        """
        __init__

        """
        myThread = threading.currentThread()
        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi
            
        DBCreator.__init__(self, logger, dbi)
        
        self.delete["01test_tablea"] = "DROP TABLE test_tablea"
        self.delete["02test_tableb"] = "DROP TABLE test_tableb"
        self.delete["03test_tablec"] = "DROP TABLE test_tablec"

        return
