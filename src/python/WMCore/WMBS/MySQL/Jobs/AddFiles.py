#!/usr/bin/env python
"""
_AddFiles_

MySQL implementation of Jobs.AddFiles
"""

import logging

from WMCore.Database.DBFormatter import DBFormatter

class AddFiles(DBFormatter):

    sql = """INSERT INTO wmbs_job_assoc (job, fileid)
               VALUES (:jobid, :fileid) """

    def getBinds(self, jobDict):
        binds = []
        for jid in jobDict:
            #For each job
            for fileID in jobDict[jid]:
                #For each file in each job
                binds.append({'jobid': jid, 'fileid': fileID})

        return binds

    def execute(self, id = None, file = None, conn = None, transaction = False, jobDict = None):

        #Adding jobDict activates bulk mode
        #Bulk mode expect jobDict of form {jid: fileid}

        if jobDict:
            binds = self.getBinds(jobDict)
        elif id and file:
            binds = DBFormatter.getBinds(self, jobid = id, fileid = file)
        else:
            logging.error('Jobs.AddFiles called with insufficient arguments')
            return

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
