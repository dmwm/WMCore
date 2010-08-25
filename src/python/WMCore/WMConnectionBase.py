#!/usr/bin/env python
"""
_WMBSBase_

Generic methods used by all of the WMBS classes.
"""

__revision__ = "$Id: WMConnectionBase.py,v 1.3 2009/11/24 22:44:30 sryu Exp $"
__version__ = "$Revision: 1.3 $"

import threading

from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory

class WMConnectionBase:
    """
    Generic db connection and transaction methods used by all of the WMCore classes.
    """
    def __init__(self, daoPackage, logger=None, dbi=None):
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

        try:
            getattr(myThread, "transaction")
        except AttributeError:
            myThread.transaction = Transaction()
            myThread.transaction.commit()
            
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
        
        try:
            getattr(myThread, "transaction")
        except AttributeError:
            print "no transaction"
            return None
        #if transaction is created properly conn attribute should exist
        return myThread.transaction.conn
            
    def beginTransaction(self):
        """
        _beginTransaction_

        Begin a database transaction if one does not already exist.
        """
        myThread = threading.currentThread()
        
        try:
            getattr(myThread, "transaction")
        except AttributeError:
            #This creates and begins a new transaction 
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
        try:
            getattr(myThread, "transaction")
        except AttributeError:
            return False
        
        if myThread.transaction.transaction != None:
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
