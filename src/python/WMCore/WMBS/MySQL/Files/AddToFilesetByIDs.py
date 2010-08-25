"""
MySQL implementation of Files.AddToFilesetByIDs
"""
from WMCore.Database.DBFormatter import DBFormatter

class AddToFilesetByIDs(DBFormatter):
    sql = """
            insert wmbs_fileset_files (file, fileset) 
                select :file_id, wmbs_fileset.id 
                from wmbs_fileset where wmbs_fileset.id = :fileset"""
        
    def getBinds(self, file = None, fileset = None):
        return self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset',
                                    self.dbi.buildbinds(self.dbi.makelist(file), 'file_id'))
    
    def format(self, result):
        return True
            
    def execute(self, file=None, fileset=None, conn = None, transaction = False):
        binds = self.getBinds(file, fileset)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        
        return self.format(result)