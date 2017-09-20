#!/usr/bin/env python
"""
_Transaction_

A simple wrapper around DBInterface to make working with transactions simpler

On MySQL transactions only work for innodb tables.
"""



import logging
import time

from WMCore.DataStructs.WMObject import WMObject
from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION

class Transaction(WMObject):
    dbi = None

    def __init__(self, dbinterface = None):
        """
        Get the connection from the DBInterface and open a new transaction on it
        """
        self.dbi = dbinterface
        self.conn = None
        self.transaction = None

    def begin(self):
        if self.conn == None:
            self.conn = self.dbi.connection()
        if self.conn.closed:
            self.conn = self.dbi.connection()

        if self.transaction == None:
            self.transaction = self.conn.begin()

        return

    def processData(self, sql, binds={}):
        """
        Propagates the request to the proper dbcore backend,
        and performs checks for lost (or closed) connection.
        """
        result = self.dbi.processData(sql, binds, conn = self.conn,
                                      transaction = True)
        return result

    def commit(self):
        """
        Commit the transaction and return the connection to the pool
        """
        if not self.transaction == None:
            self.transaction.commit()

        if not self.conn == None:
            self.conn.close()
        self.conn = None
        self.transaction = None

    def rollback(self):
        """
        To be called if there is an exception and you want to roll back the
        transaction and return the connection to the pool
        """
        if self.transaction:
            self.transaction.rollback()

        if self.conn:
            self.conn.close()

        self.conn = None
        self.transaction = None
        return

    def rollbackForError(self):
        """
        This is called when handling a major exception.  This is because sometimes
        you can end up in a situation where the transaction appears open, but is not.  In
        this case, calling a rollback on the transaction will cause an exception, which
        then destroys all logging and shutdown of the actual code.

        Use only in components.
        """

        try:
            self.rollback()
        except:
            pass
        return
