#!/usr/bin/env python
"""
_Transaction_

A simple wrapper around DBInterface to make working with transactions simpler


"""
__revision__ = "$Id: Transaction.py,v 1.2 2008/08/21 11:31:25 metson Exp $"
__version__ = "$Revision: 1.2 $"

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

    def processData(self, sql, binds):
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