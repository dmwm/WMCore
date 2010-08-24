"""
MySQL implementation of File.Get
"""
from WMCore.WMBS.MySQL.Files.GetByID import GetByID

class GetByLFN(GetByID):
    sql = """select file.id, file.lfn, file.size, file.events, file.cksum
             from wmbs_file_details as file  
             where lfn = :lfn"""
    #select id, lfn, size, events, run, lumi from wmbs_file_details where id = :file

    def getBinds(self, files=None):
        binds = []
        files = self.dbi.makelist(files)
        for f in files:
        	binds.append({'lfn': f})
        return binds

    def format(self, result):
        out = []
        if len(result) > 0:
            for r in result:
                f = r.fetchall()
                # Only want the first record - later ones should be prevented by
                # the schema.
                f = f[0]
                t = int(f[0]), str(f[1]), int(f[2]), int(f[3]), int(f[4])
                out.append(t)
            return out
        else:
            raise Exception, "File not found"

    def execute(self, files=None, conn = None, transaction = False):
        binds = self.getBinds(files)
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        assert len(result) == len(binds),\
             "Found %s results for %s input" % (len(result), len(binds))
        return self.format(result)



