#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information

                                                                                                                                                                                                                                                                                                                                                                                                          """
__revision__ = "$Id: SetBlockStatus.py,v 1.9 2009/12/07 18:57:21 mnorman Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "mnorman@fnal.gov"

import threading
import exceptions

from sets import Set

from WMCore.Database.DBFormatter import DBFormatter


class SetBlockStatus(DBFormatter):

    existsSQL = """
    SELECT blockname FROM dbsbuffer_block WHERE blockname = :block
    """

    sql = """INSERT INTO dbsbuffer_block (blockname, location, status, create_time)
               SELECT :block, (SELECT id FROM dbsbuffer_location WHERE se_name = :location), :status, :time FROM DUAL
               """

    updateSQL = """UPDATE dbsbuffer_block SET status = :status
                     WHERE blockname = :block 
    """

    timedSQL  = """UPDATE dbsbuffer_block SET status = :status, create_time = :time
                     WHERE blockname = :block 
    """


    def __init__(self, logger = None, dbi = None):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)


    def execute(self, block, locations = None, open_status = 0, time = 0, conn = None, transaction = False):
        """
        _execute_

        Given a block and a list of locations add entries to the dbsbuffer_block
        table.
        """
        bindVars = []

        locations = list(Set(locations))

        
        #It gets a bit weird here.
        #Basically, the DBSAPI has preset the OpenForWriting status in a block to be a string 1 or 0
        #I don't want to mess with DBSAPI, so I have to interpret this here.
        #Hence us parsing open_status into status
        status = ''

        if open_status == '1' or open_status == 1:
            status = 'Open'
        elif open_status == '0' or open_status == 0:
            status = 'InGlobalDBS'
        elif type(open_status) == str:
            status = open_status
        else:
            status = 'Unknown'

        testResult = self.dbi.processData(self.existsSQL, {'block': block}, conn = conn, transaction = transaction)

        res1 = self.formatDict(testResult)
        sql  = None

        if res1 == []:
            sql = self.sql
            for location in locations:
                bindVars.append({"block": block, "location": location, "status": status, "time": time})
        else:

            if time:
                bindVars = {"block": block, "status": status, "time": time}
                sql = self.timedSQL
            else:
                bindVars = {"block": block, "status": status}
                sql = self.updateSQL

                

        self.dbi.processData(sql, bindVars, conn = conn,
                                     transaction = transaction)
            

        return
                                             

