"""
SQLite implementation of NewFileset
"""
from WMCore.WMBS.MySQL.NewFilesetSQL import NewFilesetSQL as NewFilesetMySQL
from WMCore.WMBS.SQLite.Base import SQLiteBase

class NewFilesetSQL(NewFilesetMySQL, SQLiteBase):
    sql = """insert into wmbs_fileset 
            (name, last_update) values (:fileset, :timestamp)"""
            
    def getBinds(self, fileset = None):
        binds = self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset',
            self.dbi.buildbinds(
                self.dbi.makelist(self.timestamp()), 'timestamp'))
        return binds