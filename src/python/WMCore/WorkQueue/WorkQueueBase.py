#!/usr/bin/env python
"""
_WMBSBase_

Generic methods used by all of the WMBS classes.
"""

__revision__ = "$Id: WorkQueueBase.py,v 1.3 2009/08/18 23:18:15 swakef Exp $"
__version__ = "$Revision: 1.3 $"

import threading

from WMCore.WMConnectionBase import WMConnectionBase

class WorkQueueBase(WMConnectionBase):
    """
    Generic methods used by all of the WMBS classes.
    """
    def __init__(self):
        """
        ___init___

        Initialize all the database connection attributes and the logging
        attritbutes.  Create a DAO factory for WMCore.WorkQueue as well. Finally,
        check to see if a transaction object has been created.  If none exists,
        create one but leave the transaction closed.
        """
        WMConnectionBase.__init__(self, daoPackage = "WMCore.WorkQueue.Database")