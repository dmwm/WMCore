#!/usr/bin/env python
"""
_Save_

MySQL implementation of Masks.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.3 2009/01/11 17:44:41 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class Save(DBFormatter):
    sqlBeginning = "UPDATE wmbs_job_mask SET"
    sqlEnding = " WHERE JOB = :jobid"

    def execute(self, jobid, mask, conn = None, transaction = False):
        doUpdate = False
        binds = {"jobid": jobid}
        sql = self.sqlBeginning

        for maskKey in mask.keys():
            if mask[maskKey] != None:
                doUpdate = True
                binds[maskKey] = mask[maskKey]
                sql += " %s = :%s," % (maskKey, maskKey)

        sql = sql[:-1] + self.sqlEnding

        if doUpdate:
            self.dbi.processData(sql, binds, conn = conn,
                                 transaction = transaction)
