#!/usr/bin/env python
"""
_WMBSBase_

Generic methods used by all of the WMBS classes.
"""

__revision__ = "$Id: WMBSBase.py,v 1.1 2009/01/08 21:50:50 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory

class WMBSBase:
    """
    Generic methods used by all of the wmbs classes.
    """
    def __init__(self):
        """
        ___init___

        Initialize all the database connection attributes and the logging
        attritbutes.  Create a DAO factory for WMCore.WMBS as well. Finally,
        check to see if a transaction object has been created.  If none exists,
        create one but leave the transaction closed.
        """
        self.myThread = threading.currentThread()
        self.logger = self.myThread.logger
        self.dialect = self.myThread.dialect
        self.dbi = self.myThread.dbi
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = self.logger,
                                     dbinterface = self.dbi)

        if "transaction" not in dir(self.myThread):
            self.myThread.transaction = Transaction(self.dbi)
            self.myThread.transaction.commit()

        self.newTrans = False
        return

    def getReadDBConn(self):
        """
        _getReadDBConn_

        Retrieve a "Read" database connection.  If there is an existing
        transaction this will retrieve the database connection from the
        transaction.  If no transaction exists None will be returned and it
        will be up to the database class to create the connection.
        """
        return self.myThread.transaction.conn

    def getWriteDBConn(self):
        """
        _getWriteDBConn_

        Retrieve a "Write" database connection.  If there is an existing
        transaction this will retrieve the database connection from the
        transaction.  If no transaction exists a new one will be created and
        the connection from the new transaction will be returned.  The
        transaction will be committed once commitIfNew() is called.
        """
        if self.myThread.transaction.conn == None:
            self.newTrans = True
            self.myThread.transaction.begin()

        return self.myThread.transaction.conn

    def beginTransaction(self):
        """
        _beginTransaction_

        If there is no active transaction begin a new one and set newTrans to
        True.  If there is an active transaction do nothing.
        """
        if self.myThread.transaction.conn == None:
            self.newTrans = True
            self.myThread.transaction.begin()

        return

    def existingTransaction(self):
        """
        _existingTransaction_

        Return True if there is an open transaction, False otherwise.
        """
        if self.myThread.transaction.conn != None:
            return True

        return False

    def commitIfNew(self):
        """
        _commitIfNew_

        If there is an open transaction that was created by the getWriteDBConn()
        method commit it to the database, return otherwise.
        """
        if self.newTrans:
            self.myThread.transaction.commit()
            self.newTrans = False

        return
