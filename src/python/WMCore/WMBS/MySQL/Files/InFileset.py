"""
MySQL implementation of Files.InFileset
"""
from WMCore.Database.DBFormatter import DBFormatter

class InFileset(DBFormatter):
    sql = """select distinct file.id, file.lfn, file.size, file.events, map.run, map.lumi
            from wmbs_file_details as file join wmbs_file_runlumi_map as map on map.file = file.id 
            where id in (select file from wmbs_fileset_files where 
            fileset = (select id from wmbs_fileset where name = :fileset))"""
                
    def getBinds(self, fileset = None):
        return self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset')
            
    def execute(self, fileset=None, conn = None, transaction = False):
        binds = self.getBinds(fileset)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)