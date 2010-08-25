#!/usr/bin/env python
"""
_New_

MySQL implementation of Masks.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.4 2009/09/09 21:06:59 mnorman Exp $"
__version__ = "$Revision: 1.4 $"

import logging

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wmbs_job_mask (job, inclusivemask) VALUES
             (:jobid, :inclusivemask)"""
    
    def format(self,result):
        return True

    def getDictBinds(self, jobList, inclusivemask = True):
        binds = []
        for job in jobList:
            binds.append({'jobid': job['id'], 'inclusivemask': inclusivemask})

        return binds
    
    def execute(self, jobid = None, inclusivemask = None, conn = None,
                transaction = False, jobList = None):

        if jobList:
            binds = self.getDictBinds(jobList, inclusivemask)
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return self.format(result)
            
        elif jobid:
            if inclusivemask == None:
                binds = self.getBinds(jobid = jobid, inclusivemask=True)
            else:
                binds = self.getBinds(jobid = jobid, inclusivemask = inclusivemask)
            
            result = self.dbi.processData(self.sql, binds, conn = conn,
                                          transaction = transaction)
            return self.format(result)

        else:
            logging.error('Masks.New asked to create Mask with no Job ID')
            return


