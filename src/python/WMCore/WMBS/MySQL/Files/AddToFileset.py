"""
MySQL implementation of Files.AddToFileset
"""
from WMCore.Database.DBFormatter import DBFormatter

class AddToFileset(DBFormatter):
    sql = """
            insert wmbs_fileset_files (file, fileset) 
                select wmbs_file_details.id, wmbs_fileset.id 
                from wmbs_file_details, wmbs_fileset 
                where wmbs_file_details.lfn = :lfn
                and wmbs_fileset.name = :fileset"""
        
    def getBinds(self, file = None, fileset = None):
        return self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset',
                                    self.dbi.buildbinds(self.dbi.makelist(file), 'lfn'))
    
    def format(self, result):
        return True
            
    def execute(self, file=None, fileset=None, conn = None, transaction = False):
        binds = self.getBinds(file, fileset)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        
        return self.format(result)