#!/usr/bin/env python
"""
_Transaction_

A simple wrapper around DBInterface to make working with transactions simpler

On MySQL transactions only work for innodb tables.

On SQLite transactions only work if isolation_level is not null. This can be set
in the DBFactory class by passing in options={'isolation_level':'DEFERRED'}. If
you set {'isolation_level':None} all sql will be implicitly committed and the 
Transaction object will be meaningless.
"""
__revision__ = "$Id: Transaction.py,v 1.3 2008/08/21 16:11:22 metson Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.DataStructs.WMObject import WMObject

class Transaction(WMObject):
    dbi = None
    
    def __init__(self, dbinterface = None):
        """
        Get the connection from the DBInterface and open a new transaction on it
        """
        self.dbi = dbinterface
        self.conn = self.dbi.connection()
        self.transaction = self.conn.begin()

    def processData(self, sql, binds={}):
        return self.dbi.processData(sql, 
                                    binds, 
                                    conn = self.conn, 
                                    transaction = True)
        
    def commit(self):
        """
        Commit the transaction and return the connection to the pool
        """
        self.transaction.commit()
        self.conn.close()
        
    def rollback(self):
        """
        To be called if there is an exception and you want to roll back the 
        transaction and return the connection to the pool
        """
        self.transaction.rollback()
        self.conn.close()