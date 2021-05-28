#!/usr/bin/env python
"""
_WMBSBase_

Generic methods used by all of the WMBS classes.
"""

from builtins import object

import threading

class WorkQueueBase(object):
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
        # only load dbi connection if we need it
        if dbi or 'dbi' in dir(threading.currentThread()):
            from WMCore.WMConnectionBase import WMConnectionBase
            self.conn = WMConnectionBase(daoPackage = "WMCore",
                                         logger = logger, dbi = dbi)
            self.logger = self.conn.logger
        else:
            self.conn = None
            if logger:
                self.logger = logger
            elif 'logger' in dir(threading.currentThread()):
                self.logger = threading.currentThread().logger
            else:
                import logging
                self.logger = logging.getLogger()
