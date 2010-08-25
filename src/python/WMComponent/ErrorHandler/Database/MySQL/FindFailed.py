#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_FindFailed_

This module implements the mysql backend for the 
errorhandler, for locating the fialed jobs in certain state

"""

__revision__ = \
    "$Id: FindFailed.py,v 1.1 2009/05/08 16:32:40 afaq Exp $"
__version__ = \
    "$Revision: 1.1 $"
__author__ = \
    "anzar@fnal.gov"

import threading

from WMCore.Database.DBFormatter import DBFormatter

class FindFailed(DBFormatter):
    """
    This module implements the mysql backend for the 
    create job error handler.
    
    """

    sqlStr = """SELECT ID FROM wmbs_job, jsm_state WHERE status = :job_status limit 100"""
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
    
    def getBinds(self, jobStatus):
        print dataset
        binds =  { 'job_status': jobStatus}
        return binds
   
    def formatDict(self, result):
        """
        _formatDict_

        Cast the id, jobgroup and last_update columns to integers because
        formatDict() turns everything into strings.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"] = int(formattedResult["id"])
        return formattedResult
 
    def execute(self, jobStatus, conn=None, transaction = False):
        binds = self.getBinds(jobStatus)
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return result

