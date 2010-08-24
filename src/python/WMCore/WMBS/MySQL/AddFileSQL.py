"""
MySQL implementation of AddFile
"""
from WMCore.WMBS.MySQL.Base import MySQLBase

class AddFile(object):
    sql = """insert into wmbs_file_details (lfn, size, events, run, lumi) 
                values (:lfn, :size, :events, :run, :lumi)"""
                
    def getBinds(self, files=None, size=0, events=0, run=0, lumi=0):
        # Can't use self.dbi.buildbinds here...
        binds = {}
        if type(files) == type('string'):
            binds = {'lfn': files, 
                     'size': size, 
                     'events': events, 
                     'run': run, 
                     'lumi':lumi} 
        elif type(files) == type([]):
        # files is a list of tuples containing lfn, size, events, run and lumi
            binds = []
            for f in files:
                binds.append({'lfn': f[0], 
                              'size': f[1], 
                              'events': f[2], 
                              'run': f[3], 
                              'lumi':f[4]})
        return binds
    
    def execute(self, files=None, size=0, events=0, run=0, lumi=0, conn = None, transaction = False):
        binds = self.getBinds(files, size, events, run, lumi)
        
        self.logger.debug('AddFile binds: %s' % binds)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)