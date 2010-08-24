#!/usr/bin/env python

"""
_Queries_

This module implements the mysql backend for the message
service.

"""

__revision__ = \
    "$Id: Queries.py,v 1.1 2008/08/26 13:55:56 fvlingen Exp $"
__version__ = \
    "$Revision: 1.1 $"
__author__ = \
    "fvlingen@caltech.edu"

import threading

from WMCore.Database.DBFormatter import DBFormatter

class Queries(DBFormatter):
    """
    _Queries_
    
    This module implements the mysql backend for the message
    service.
    
    """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        
    def checkName(self, args):
        """
        __checkName__

        Checks the name of the component in the backend.
        """

        sqlStr = """ 
SELECT procid, host, pid FROM ms_process WHERE name = :name 
""" 
        result = self.execute(sqlStr, args)
        return self.formatOne(result)

    def updateName(self, args):
        """
        __updateName__

        Updates the name of the component in the backend.
        """

        sqlStr = """
UPDATE ms_process SET pid = :currentPid, host = :currentHost WHERE name = :name
"""
        self.execute(sqlStr, args)

    def insertProcess(self, args):
        """
        __insertProcess__

        Inserts the name of the component in the backend
        """

        sqlStr = """
INSERT INTO ms_process(host,pid,name) VALUES (:host,:pid,:name)
"""
        self.execute(sqlStr, args)

    def lastInsertId(self, args = {}):
        """
        __lastInsertId__

        Checks for last inserted id 
        """

        sqlStr = """
SELECT LAST_INSERT_ID()
"""
        result = self.execute(sqlStr, args)
        return self.formatOne(result)

    def execute(self, sqlStr, args):
        """"
        __execute__
        Executes the queries by getting the current transaction
        and dbinterface object that is stored in the reserved words of
        the thread it operates in.
        """
        myThread = threading.currentThread()
        currentTransaction = myThread.transaction
        return currentTransaction.processData(sqlStr, args) 


