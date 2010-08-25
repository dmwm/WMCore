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
__revision__ = "$Id: Transaction.py,v 1.9 2009/07/28 12:52:15 sfoulkes Exp $"
__version__ = "$Revision: 1.9 $"

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
        self.begin()
        # retries when disconnect
        self.retries = 5
        # buffer for losing connection
        self.sqlBuffer = []
       
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
        self.sqlBuffer.append( (sql, binds) )
        # but we might dealing with a closed or
        # lost connection.
        if not self.conn == None:
            if self.conn.closed:
                self.conn = None
        try:
            result = self.dbi.processData(sql, binds, conn = self.conn, \
                transaction = True)
        except Exception, ex:
            logging.warning("Problem connecting to database: "+str(ex))
            logging.warning("Be patient!")
            logging.warning("Trying to close existing connection")
            try:
                self.conn.close()
            except:
                pass
            logging.warning("Trying to reconnect")
            result = self.redo()

        return result

    def commit(self):
        """
        Commit the transaction and return the connection to the pool
        """
        if not self.transaction == None:
            try:
                self.transaction.commit()
            except Exception, ex:
                print "PROBLEM: %s" % str(ex)
                logging.warning("Problem connecting to database: "+str(ex))
                logging.warning("Be patient!")
                logging.warning("Trying to close existing connection")
                try:
                    self.conn.close()
                except:
                    pass
                logging.warning("Trying to reconnect")
                self.redo()
                self.transaction.commit()
        else:
            logging.debug("Transaction::commit(%s) had null transaction object" %id(self))
            
            
        self.sqlBuffer = []
        if not self.conn == None:
            self.conn.close()
        self.conn = None
        self.transaction = None
       
    def rollback(self):
        """
        To be called if there is an exception and you want to roll back the
        transaction and return the connection to the pool
        """
        self.sqlBuffer = []
        self.transaction.rollback()
        self.conn.close()
        self.conn = None
        self.transaction = None

    def redo(self):
        """
        Tries to re-execute all statements that where not committed,
        before the connection was lost.
        """
        #Added by mnorman for testing.  Do NOT commit
        #for sql, binds in self.sqlBuffer:
        #    print 'Going to attempt redo for %s, %s' %(sql, binds)
        tries = 0
        result = None
        connectionProblem = True
        fatalError = False
        ## try several time to connect.
        waitTime = 1
        reportedError = ''
        while tries < self.retries and connectionProblem:
            #try reconnecting
            self.begin()
            try:
                for sql, binds in self.sqlBuffer:
                    result = self.dbi.processData(sql, binds, \
                        conn = self.conn, transaction = True)
                tries = self.retries
                connectionProblem = False
            except Exception,ex:
                logging.warning("Again error. "+str(ex))
                logging.warning(str(self.retries - tries)+" tries left")
                reportedError = str(ex)
                #if connection problem:
                tries += 1
                if tries > self.retries:
                    fatalError = True
                ##else:
                ##connectionProblem = False
                ##tries = self.retries
            # wait a few seconds if connection failed again:
            # multiply wait time.
            if connectionProblem:
                waitTime = waitTime * 3
                logging.warning("Waiting: "+str(waitTime)+" seconds before retry")
                time.sleep(waitTime)
        if connectionProblem or fatalError:
            raise WMException(WMEXCEPTION['WMCORE-12']+str(reportedError), \
                'WMCORE-12')
        return result
