"""
MySQL implementation of File.Get
"""
from WMCore.WMBS.MySQL.Files.GetByID import GetByID

class GetByLFN(GetByID):
    sql = """select files.id, files.lfn, files.filesize, files.events, files.cksum, ds.Path, files.status
             from dbsbuffer_file as files
		join dbsbuffer_dataset ds
			on files.dataset=ds.ID
             where lfn = :lfn"""

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
                t = int(f[0]), str(f[1]), int(f[2]), int(f[3]), int(f[4]), str(f[5]), str(f[6])
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



