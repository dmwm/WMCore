"""
MySQL implementation of File.GetLocation
"""
from WMCore.Database.DBFormatter import DBFormatter
from sets import Set

class GetLocation(DBFormatter):
    sql = """select se_name from wmbs_location 
                where id in (select location from wmbs_file_location 
                    where file in (select id from wmbs_file_details where lfn=:lfn))"""
                    
    
    def getBinds(self, file=None):
        binds = []
        file = self.dbi.makelist(file)
        for f in file:
            binds.append({'lfn': f})
        return binds
    
    def format(self, result):
        "Return a list of SE FQDN's"
        out = Set()
        for r in result:
            for i in r.fetchall():
                out.add(i[0])
            r.close()
        return out
    
    def execute(self, file=None, conn = None, transaction = False):
        binds = self.getBinds(file)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)
