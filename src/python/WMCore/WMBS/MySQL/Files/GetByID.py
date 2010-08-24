"""
MySQL implementation of File.Get
"""
from WMCore.WMBS.MySQL.Base import MySQLBase

class GetByID(MySQLBase):
    sql = """select file.id, file.lfn, file.size, file.events, map.run, map.lumi
             from wmbs_file_details as file join wmbs_file_runlumi_map as map on map.file = file.id 
             where file.id = :file"""
    
    def getBinds(self, files=None):
        binds = []
        files = self.dbi.makelist(files)
        for f in files:
            binds.append({'file': f})
        return binds
    
    def format(self, result):
        out = result[0].fetchall()
        self.logger.debug('File.GetByID format result: %s' % out)
        if len(out) > 0:
            out = out[0]
            out = int(out[0]), str(out[1]), int(out[2]), int(out[3]), int(out[4]), int(out[5])
            return out
        else:
            raise Exception, "File not found" 
        
    def execute(self, files=None, conn = None, transaction = False):
        binds = self.getBinds(files)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)