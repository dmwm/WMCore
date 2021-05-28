#!/usr/bin/env python
"""
_GetRunLumiFile_

MySQL implementation of GetRunLumiFile
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetRunLumiFile(DBFormatter):
    sql = """SELECT flr.run AS run, flr.lumi AS lumi, flr.num_events as num_events
               FROM wmbs_file_runlumi_map flr
               INNER JOIN wmbs_file_details wfd ON wfd.id = flr.fileid
               WHERE wfd.lfn = :lfn"""


    def getBinds(self, file=None):
        binds = []
        file = self.dbi.makelist(file)
        for f in file:
            binds.append({'lfn': f})
        return binds

    def format(self, result):
        "Return a list of Run/Lumi Set"
        run_lumis={}
        for r in result:
            for i in r.fetchall():
                if i[0] not in run_lumis:
                    run_lumis[i[0]]=[]
                run_lumis[i[0]].append((i[1], i[2]))
                r.close()
        return run_lumis

    def execute(self, file=None, conn = None, transaction = False):
        binds = self.getBinds(file)

        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
