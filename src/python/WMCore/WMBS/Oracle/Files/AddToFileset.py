"""
SQLite implementation of AddFileToFileset
"""
from WMCore.WMBS.MySQL.Files.AddToFileset import AddToFileset as AddFileToFilesetMySQL

class AddToFileset(AddFileToFilesetMySQL):
    """
    _AddToFileset_
    
    overwirtes MySQL Files.AddRunLumi.sql to use in oracle.
    
    To Check: might not need to overwrite if modifiy MySQL.Files.AddRunLumi.sql
    a bit to work with both also remove the getBinds method  
    """
    
    sql = """insert into wmbs_fileset_files 
            (fileid, fileset, insert_time) 
            values ((select id from wmbs_file_details where lfn = :lfn),
            (select id from wmbs_fileset where name = :fileset), :timestamp)"""
                    
    def getBinds(self, file = None, fileset = None):
        return self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset',
                        self.dbi.buildbinds(self.dbi.makelist(file), 'lfn',
                                self.dbi.buildbinds(
                                    self.dbi.makelist(self.timestamp()), 'timestamp')))
    