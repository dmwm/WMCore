"""
Oracle implementation of AddFileToFilesetByIDs
"""
from WMCore.WMBS.MySQL.Files.AddToFilesetByIDs import AddToFilesetByIDs as AddFileToFilesetByIDsMySQL

class AddToFilesetByIDs(AddFileToFilesetByIDsMySQL):
    """
    _AddToFilesetByIDs_
    
    overwirtes MySQL Files.AddToFilesetByIDs.sql to use in oracle.
    
    """
    
    sql = """insert into wmbs_fileset_files 
            (fileid, fileset, insert_time) 
            values (:file_id,
            (select id from wmbs_fileset where name = :fileset), :timestamp)"""
                    
    def getBinds(self, file = None, fileset = None):
        return self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset',
                        self.dbi.buildbinds(self.dbi.makelist(file), 'file_id',
                                self.dbi.buildbinds(
                                    self.dbi.makelist(self.timestamp()), 'timestamp')))
    