#!/usr/bin/env python
"""
_DBSBuffer.UpdateFileStatus_

Update Algo status in a Dataset to promoted

"""
__revision__ = "$Id: UpdateFilesStatus.py,v 1.1 2008/11/18 23:25:30 afaq Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"

import threading
import exceptions

from WMCore.Database.DBFormatter import DBFormatter

class UpdateFileStatus(DBFormatter):

    sql = """UPDATE dbsbuffer_file SET FileStatus = :status where LFN IN (???????????)Path=:path"""

    sql_delme = """UPDATE dbsbuffer_dataset as A
                   inner join (
                      select * from dbsbuffer_dataset
                          where Path=:path
                   ) as B on A.ID = B.ID
                SET A.AlgoInDBS = 1"""

    #sqlUpdateDS = """UPDATE dbsbuffer_dataset SET UnMigratedFiles = UnMigratedFiles + 1 WHERE ID = (select ID from dbsbuffer_dataset where Path=:path)"""
    def __init__(self):
            myThread = threading.currentThread()
            DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        
    def getBinds(self, dataset=None):
            # binds a list of dictionaries
           binds =  { 
            'path': dataset['Path']
            }
           
           return binds
       
    def format(self, result):
        return True

    def execute(self, dataset=None, conn=None, transaction = False):
        
        binds = self.getBinds(dataset)

        try:
            result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)

        except Exception, ex:
                raise ex
            