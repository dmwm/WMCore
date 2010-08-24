"""
MySQL implementation of AddFile
"""
from WMCore.Database.DBFormatter import DBFormatter

class Add(DBFormatter):
    sql = """insert into wmbs_file_details (lfn, size, events, cksum) 
                values (:lfn, :filesize, :events, :cksum)"""
                
    def getBinds(self, files=None, size=0, events=0, cksum=0):
        # Can't use self.dbi.buildbinds here...
        binds = {}
        if type(files) == type('string'):
            binds = {'lfn': files, 
                     'filesize': size, 
                     'events': events,
		     'cksum' : cksum} 
        elif type(files) == type([]):
        # files is a list of tuples containing lfn, size, events, run and lumi
            binds = []
            for f in files:
                binds.append({'lfn': f[0], 
                              'filesize': f[1], 
                              'events': f[2],
				'cksum' : f[3]
				})
        return binds
    
    def format(self, result):
        return True
    
    def execute(self, files = None, size = 0, events = 0, cksum = 0,
                conn = None, transaction = False):
        binds = self.getBinds(files, size, events, cksum)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)
