"""
MySQL implementation of GetRunLumiFile
"""
from WMCore.Database.DBFormatter import DBFormatter
from sets import Set

class GetRunLumiFile(DBFormatter):
    sql = """select flr.run as run, flr.lumi as lumi
		from wmbs_file_runlumi_map flr 
			where flr.file in (select id from wmbs_file_details where lfn=:lfn)"""

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
                if i[0] not in run_lumis.keys():
                    run_lumis[i[0]]=[]
                run_lumis[i[0]].append(i[1])
        	r.close()
        return run_lumis

    def execute(self, file=None, conn = None, transaction = False):
        binds = self.getBinds(file)

        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)



