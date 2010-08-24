#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_Queries_

This module implements the mysql backend for the 
FeederManage

"""

import time

__revision__ = \
    "$Id: Queries.py,v 1.2 2009/02/02 23:37:35 jacksonj Exp $"
__version__ = \
    "$Revision: 1.2 $"
__author__ = \
    "james.jackson@cern.ch"

import threading

from WMCore.Database.DBFormatter import DBFormatter

class Queries(DBFormatter):
    """
    _Queries_
    
    This module implements the MySQL backend for the FeederManager
    
    """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        

    # FIXME: these are all single inserts
    # find a way to do this in bulk.
    # we can do this if we enable a thread slave
    # retrieve messages from the queu in bulk.   
    def addFeeder(self, feederType, feederState):
        """
        Adds a managed feeder
        """
        sqlStr = """
INSERT INTO managed_feeders(feeder_type, feeder_state, insert_time)
VALUES (:type, :state, :time) 
"""
        self.execute(sqlStr, {'type' : feederType, 'state' : feederState, \
                              'time' : int(time.time())})

    def checkFeeder(self, feederType):
        """
        Checks if a given feeder type is already instantiated
        """
        sqlStr = """
SELECT COUNT(*) FROM managed_feeders WHERE feeder_type = :type"""
        result = self.execute(sqlStr, {'type':feederType})
        return self.formatOne(result)[0] != 0
    
    def getFeederId(self, feederType):
        """
        Gets the ID for a given feeder
        """
        sqlStr = """
SELECT id from managed_feeders WHERE feeder_type = :type"""
        result = self.execute(sqlStr, {"type" : feederType})
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
