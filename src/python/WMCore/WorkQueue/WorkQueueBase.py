#!/usr/bin/env python
"""
_WMBSBase_

Generic methods used by all of the WMBS classes.
"""

__revision__ = "$Id: WorkQueueBase.py,v 1.2 2009/06/24 21:00:23 sryu Exp $"
__version__ = "$Revision: 1.2 $"

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