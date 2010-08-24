"""
MySQL implementation of AddFile
"""
from WMCore.Database.DBFormatter import DBFormatter

class Add(DBFormatter):
    sql = """insert into wmbs_file_details (lfn, size, events, cksum,
                                            first_event, last_event)
             values (:lfn, :filesize, :events, :cksum, :first_event,
                     :last_event)"""
                
    def getBinds(self, files = None, size = 0, events = 0, cksum = 0,
                 first_event = 0, last_event = 0):
        # Can't use self.dbi.buildbinds here...
        binds = {}
        if type(files) == type('string'):
            binds = {'lfn': files, 
                     'filesize': size, 
                     'events': events,
		     'cksum' : cksum,
                     'first_event' : first_event,
                     'last_event' : last_event} 
        elif type(files) == type([]):
        # files is a list of tuples containing lfn, size, events, run and lumi
            binds = []
            for f in files:
                binds.append({'lfn': f[0], 
                              'filesize': f[1], 
                              'events': f[2],
                              'cksum' : f[3],
                              'first_event' : f[4],
                              'last_event' : f[5]
				})
        return binds
    
    def execute(self, files = None, size = 0, events = 0, cksum = 0,
                first_event = 0, last_event = 0, conn = None,
                transaction = False):
        binds = self.getBinds(files, size, events, cksum, first_event,
                              last_event)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return
