#!/usr/bin/env python
"""
_WMBSBase_

Generic methods used by all of the WMBS classes.
"""

__revision__ = "$Id: WMConnectionBase.py,v 1.1 2009/06/12 16:37:26 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory

class WMConnectionBase:
    """
    Generic db connection and transaction methods used by all of the WMCore classes.
    """
    def __init__(self, daoPackage):
        """
        ___init___

        Initialize all the database connection attributes and the logging
        attritbutes.  Create a DAO factory for given daoPackage as well. Finally,
        check to see if a transaction object has been created.  If none exists,
        create one but leave the transaction closed.
        """
        self.myThread = threading.currentThread()
        self.logger = self.myThread.logger
        self.dialect = self.myThread.dialect
        self.dbi = self.myThread.dbi
        self.daofactory = DAOFactory(package = daoPackage,
                                     logger = self.logger,
                                     dbinterface = self.dbi)

        if "transaction" not in dir(self.myThread):
            self.myThread.transaction = Transaction(self.dbi)
            self.myThread.transaction.commit()

        return

    def getDBConn(self):
        """
        _getDBConn_

        Retrieve the database connection that is associated with the current
        dataabase transaction.
        """
        return self.myThread.transaction.conn

    def beginTransaction(self):
        """
        _beginTransaction_

        Begin a database transaction if one does not already exist.
        """
        if self.myThread.transaction.conn == None:
            self.myThread.transaction.begin()
            return False

        return True

    def existingTransaction(self):
        """
        _existingTransaction_

        Return True if there is an open transaction, False otherwise.
        """
        if self.myThread.transaction.conn != None:
            return True

        return False

    def commitTransaction(self, existingTransaction):
        """
        _commitTransaction_

        Commit a database transaction that was begun by self.beginTransaction().
        """
        if not existingTransaction:
            self.myThread.transaction.commit()
            self.myThread.transaction.conn = None

        return
