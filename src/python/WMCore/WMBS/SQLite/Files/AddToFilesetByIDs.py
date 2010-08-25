"""
SQLite implementation of AddFileToFilesetByIDs
"""
from WMCore.WMBS.MySQL.Files.AddToFilesetByIDs import AddToFilesetByIDs as AddFileToFilesetByIDsMySQL

class AddToFilesetByIDs(AddFileToFilesetByIDsMySQL):
    sql = """insert into wmbs_fileset_files 
            (file, fileset, insert_time) 
            values (:file_id, :fileset, :timestamp)"""
                    
    def getBinds(self, file = None, fileset = None):
        binds = self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset',
                                self.dbi.buildbinds(
                                            self.dbi.makelist(file), 'file_id',
                                    self.dbi.buildbinds(
                                        self.dbi.makelist(
                                              self.timestamp()), 'timestamp')))
        return binds
