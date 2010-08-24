"""
SQLite implementation of AddFile
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Files.Add import Add as AddFileMySQL

class Add(AddFileMySQL, SQLiteBase):
    sql = AddFileMySQL.sql
    
    
    def getBinds(self, files=None, size=0, events=0):
        # Can't use self.dbi.buildbinds here...
        binds = {}
        if type(files) == type('string'):
            binds = {'lfn': files, 
                     'size': size, 
                     'events': events} 
        elif type(files) == type([]):
        # files is a list of tuples containing lfn, size, events, run and lumi
            binds = []
            for f in files:
                binds.append({'lfn': f[0], 
                              'size': f[1], 
                              'events': f[2]})
        return binds