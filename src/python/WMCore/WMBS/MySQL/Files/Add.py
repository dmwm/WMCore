"""
MySQL implementation of AddFile
"""
from WMCore.Database.DBFormatter import DBFormatter

class Add(DBFormatter):
    sql = """insert into wmbs_file_details (lfn, size, events, 
                                            first_event, last_event, merged)
             values (:lfn, :filesize, :events, :first_event,
                     :last_event, :merged)"""
                
    def getBinds(self, files = None, size = 0, events = 0, cksum = 0,
                 first_event = 0, last_event = 0, merged = False):
        # Can't use self.dbi.buildbinds here...
        binds = {}
        if type(files) != list:
            binds = {'lfn': files, 
                     'filesize': size, 
                     'events': events,
                     'first_event' : first_event,
                     'last_event' : last_event,
                     'merged' : int(merged)} 
        else:
            # files is a list of tuples containing lfn, size, events, run and lumi
            binds = []
            for f in files:
                binds.append({'lfn': f[0], 
                              'filesize': f[1], 
                              'events': f[2],
                              'first_event' : f[4],
                              'last_event' : f[5],
                              'merged' : int(f[6])
				})
        return binds
    
    def execute(self, files = None, size = 0, events = 0, cksum = 0,
                first_event = 0, last_event = 0, merged = False, conn = None,
                transaction = False):
        binds = self.getBinds(files, size, events, cksum, first_event,
                              last_event, merged)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return
