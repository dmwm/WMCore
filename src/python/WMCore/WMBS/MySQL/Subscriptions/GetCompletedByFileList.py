#!/usr/bin/env python
"""
_GetCompletedByFileList_

MySQL implementation of Subscription.IsFileCompleted
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class GetCompletedByFileList(DBFormatter):

    """
    returns list of file ids which are in complete status by given list of files
    If it returns the same list as input i
    """
    sql = """SELECT fileid FROM wmbs_sub_files_complete
                  WHERE subscription = :subscription AND fileid = :fileid
           """

    def format(self, result):
        out = []
        for r in result:
            if isinstance(r, int):
                # deal with crappy mysql implementation
                out.append(int(r))
            else:
                for f in r.fetchall():
                    out.append(int(f[0]))
        return out

    def getBinds(self, subscription, files):
        binds = []
        for f in files:
            binds.append({'subscription': subscription,  'fileid': f['id']})
        return binds

    def execute(self, subscription, files, conn = None,
                transaction = False):
        results = self.dbi.processData(self.sql, self.getBinds(subscription, files),
                             conn = conn, transaction = transaction)
        return self.format(results)
