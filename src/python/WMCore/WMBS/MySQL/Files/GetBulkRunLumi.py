#!/usr/bin/env python
"""
_GetBulkRunLumi_

MySQL implementation of GetBulkRunLumi
"""

from WMCore.Database.DBFormatter import DBFormatter


class GetBulkRunLumi(DBFormatter):
    """
    Note that this is ID based.  I may have to change it back
    to lfn based.

    """
    sql = """SELECT flr.run AS run, flr.lumi AS lumi, flr.fileid AS id
               FROM wmbs_file_runlumi_map flr
               WHERE flr.fileid = :id
    """

    def getBinds(self, files=None):
        binds = []
        files = self.dbi.makelist(files)
        for f in files:
            binds.append({'id': f['id']})
        return binds

    def format(self, result):
        "Return a list of Run/Lumi Set"

        finalResult = {}
        res = self.formatDict(result)

        for entry in res:
            fileid = entry['id']
            run = entry['run']
            finalResult.setdefault(fileid, {})
            finalResult[fileid].setdefault(run, [])
            finalResult[fileid][run].append(entry['lumi'])

        return finalResult

    def execute(self, files=None, conn=None, transaction=False):
        binds = self.getBinds(files)

        result = self.dbi.processData(self.sql, binds,
                                      conn=conn, transaction=transaction)
        return self.format(result)
