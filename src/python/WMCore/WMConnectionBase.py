#!/usr/bin/env python
"""
_WMBSBase_

Generic methods used by all of the WMBS classes.
"""




from builtins import object
import threading
import copy

from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory

try:
    from contextlib import contextmanager
except (ImportError, NameError):
    pass

class WMConnectionBase(object):
    """
    Generic db connection and transaction methods used by all of the WMCore classes.
    """
    def __init__(self, daoPackage, logger = None, dbi = None):
        """
        ___init___

        Initialize all the database connection attributes and the logging
        attritbutes.  Create a DAO factory for given daoPackage as well. Finally,
        check to see if a transaction object has been created.  If none exists,
        create one but leave the transaction closed.
        """
        myThread = threading.currentThread()
        if logger:
            self.logger = logger
        else:
            self.logger = myThread.logger
        if dbi:
            self.dbi = dbi
        else:
            self.dbi = myThread.dbi

        self.daofactory = DAOFactory(package = daoPackage,
                                     logger = self.logger,
                                     dbinterface = self.dbi)

        if "transaction" not in dir(myThread):
            myThread.transaction = Transaction(self.dbi)

        return

    def getDBConn(self):
        """
        _getDBConn_

        Retrieve the database connection that is associated with the current
        dataabase transaction.
        It transaction exists, it will return connection
        which that transaction belong to.
        This won't create the transaction if it doesn't exist, it will just return
        None.
        """
        myThread = threading.currentThread()

        if "transaction" not in dir(myThread):
            return None

        return myThread.transaction.conn

    def beginTransaction(self):
        """
        _beginTransaction_

        Begin a database transaction if one does not already exist.
        """
        myThread = threading.currentThread()

        if "transaction" not in dir(myThread):
            myThread.transaction = Transaction(self.dbi)
            return False

        if myThread.transaction.transaction == None:
            myThread.transaction.begin()
            return False

        return True

    def existingTransaction(self):
        """
        _existingTransaction_

        Return True if there is an open transaction, False otherwise.
        """
        myThread = threading.currentThread()

        if "transaction" not in dir(myThread):
            return False
        elif myThread.transaction.transaction != None:
            return True

        return False

    def commitTransaction(self, existingTransaction):
        """
        _commitTransaction_

        Commit a database transaction that was begun by self.beginTransaction().
        """
        if not existingTransaction:
            myThread = threading.currentThread()
            myThread.transaction.commit()

        return

    def __getstate__(self):
        """
        __getstate__

        The database connection information isn't pickleable, so we to kill that
        before we attempt to pickle.
        """
        self.dbi = None
        self.logger = None
        self.daofactory = None
        return self.__dict__


    def transactionContext(self):
        """
        Returns a transaction as a ContextManager

        Usage:
            with transactionContext():
                databaseCode1()
                databaseCode2()

        Equates to beginTransaction() followed by either
        commitTransaction or a rollback
        """
        existingTransaction = self.beginTransaction()
        try:
            yield existingTransaction
        except:
            # responsibility for rolling back is on the transaction starter
            if not existingTransaction:
                self.logger.error('Exception caught, rolling back transaction')
                threading.currentThread().transaction.rollback()
            raise
        else:
            # only commits if transaction started by this invocation
            self.commitTransaction(existingTransaction)
    try:
        transactionContext = contextmanager(transactionContext)
    except NameError:
        pass


    def rollbackTransaction(self, existingTransaction):
        """Rollback transaction if we started it"""
        if not existingTransaction:
            threading.currentThread().transaction.rollback()
