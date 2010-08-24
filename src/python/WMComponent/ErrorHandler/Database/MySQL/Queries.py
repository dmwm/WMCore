#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_Queries_

This module implements the mysql backend for the 
errorhandler.

"""

__revision__ = \
    "$Id: Queries.py,v 1.1 2008/10/02 11:10:57 fvlingen Exp $"
__version__ = \
    "$Revision: 1.1 $"
__author__ = \
    "fvlingen@caltech.edu"

import threading

from WMCore.Database.DBFormatter import DBFormatter

class Queries(DBFormatter):
    """
    _Queries_
    
    This module implements the mysql backend for the 
    error handler.
    
    """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        

    # FIXME: these are all single inserts
    # find a way to do this in bulk.
    # we can do this if we enable a thread slave
    # retrieve messages from the queu in bulk.   
    def update(self, taskid):
        """
        Updates the retry counter for a specific id, and
        returns current number of retries.
        """
        sqlStr = """
INSERT INTO err_retries(id) VALUES (:id) 
ON DUPLICATE KEY UPDATE retries=retries+1
"""
        self.execute(sqlStr, {'id':taskid})
        sqlStr = """
SELECT retries FROM err_retries WHERE id = :id
"""
        result = self.execute(sqlStr, {'id':taskid})
        return self.formatOne(result)[0]

    def remove(self, taskid):
        """
        Removes the particular id from the list.
        """
        sqlStr = """
DELETE FROM err_retries WHERE id = :id """
        self.execute(sqlStr, {'id':taskid})

    def count(self):
        """ 
        Counts the number of retries listed.
        """
        sqlStr = """
SELECT COUNT(*) FROM err_retries """
        result = self.execute(sqlStr, {})
        return self.formatOne(result)[0]

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
