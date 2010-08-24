#!/usr/bin/env python
"""
_WMBSBase_

Generic methods used by all of the WMBS classes.
"""




import threading

from WMCore.WMConnectionBase import WMConnectionBase

class WorkQueueBase(WMConnectionBase):
    """
    Generic methods used by all of the WMBS classes.
    """
    def __init__(self, logger=None, dbi=None):
        """
        ___init___

        Initialize all the database connection attributes and the logging
        attritbutes.  Create a DAO factory for WMCore.WorkQueue as well. Finally,
        check to see if a transaction object has been created.  If none exists,
        create one but leave the transaction closed.
        """
        WMConnectionBase.__init__(self, daoPackage = "WMCore.WorkQueue.Database", 
                                  logger = logger, dbi = dbi)