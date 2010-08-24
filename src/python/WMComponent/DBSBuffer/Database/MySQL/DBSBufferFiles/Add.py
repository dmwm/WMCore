"""
MySQL implementation of AddFile
"""
from WMCore.Database.DBFormatter import DBFormatter

class Add(DBFormatter):

    sql = """insert into dbsbuffer_file(lfn, size, events, cksum, dataset, status) 
                values (:lfn, :filesize, :events, :cksum, (select ID from dbsbuffer_dataset where Path=:dataset), :status)"""
                
    def getBinds(self, files=None, size=0, events=0, cksum=0, dataset=0):
        # Can't use self.dbi.buildbinds here...
        binds = {}
        if type(files) == type('string'):
            binds = {'lfn': files, 
                     'filesize': size, 
                     'events': events,
		     'cksum' : cksum,
			'dataset': dataset,
			'status' : 'NOTUPLOADED'}
        elif type(files) == type([]):
        # files is a list of tuples containing lfn, size, events, cksum, dataset, status
            binds = []
            for f in files:
                binds.append({'lfn': f[0], 
                              'filesize': f[1], 
                              'events': f[2],
				'cksum' : f[3],
				'dataset': dataset,
				'status' : 'NOTUPLOADED'
				})
        return binds
    
    def format(self, result):
        return True
    
    def execute(self, files=None, size=0, events=0, cksum=0, dataset=0, conn = None, transaction = False):

        binds = self.getBinds(files, size, events, cksum, dataset)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)
