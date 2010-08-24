#!/usr/bin/env python
"""
_DBSBuffer.UpdateFileStatus_

Update file status to promoted

"""
__revision__ = "$Id: UpdateFilesStatus.py,v 1.2 2008/11/19 19:12:35 afaq Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "anzar@fnal.gov"

import threading
import exceptions

from WMCore.Database.DBFormatter import DBFormatter

class UpdateFilesStatus(DBFormatter):

    def sql(self, files=None):
        sql = """UPDATE dbsbuffer_file SET FileStatus = :status where ID IN """
        if len(files) <= 0: raise Exception("Cannot change status of all files in DBS Buffer")
        count=0
        for afile in files:
            if count == 0:
                sql += "(:id"+str(count)
            else: sql += ",:id"+str(count)
            count += 1
        sql += ")"
        return sql

    def __init__(self):
            myThread = threading.currentThread()
            DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        
    def getBinds(self, files=None):
            # binds a list of dictionaries
           binds =  { 
            'status': 'UPLOADED',
            }
           count=0
           for afile in files:
               key="id"+str(count)
               binds[key]=afile['ID']
               count += 1
           return binds
       
    def format(self, result):
        return True

    def execute(self, files=None, conn=None, transaction = False):
                
        binds = self.getBinds(files)

        try:
            result = self.dbi.processData(self.sql(files), binds, 
                         conn = conn, transaction = transaction)

        except Exception, ex:
                raise ex
            