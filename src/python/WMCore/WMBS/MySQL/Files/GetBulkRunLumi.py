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
    sql = """SELECT flr.run AS run, flr.lumi AS lumi, flr.file AS id
               FROM wmbs_file_runlumi_map flr
               WHERE flr.file = :id
    """

    def getBinds(self, files = None):
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
            id  = entry['id']
            run = entry['run']
            if not id in finalResult.keys():
                finalResult[id] = {}
            if not run in finalResult[id].keys():
                finalResult[id][run] = []
            finalResult[id][run].append(entry['lumi'])

        return finalResult

    def execute(self, files = None, conn = None, transaction = False):
        binds = self.getBinds(files)

        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
