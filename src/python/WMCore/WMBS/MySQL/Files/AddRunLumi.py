"""
MySQL implementation of AddRunLumi
"""
from WMCore.WMBS.MySQL.Base import MySQLBase

class AddRunLumi(MySQLBase):
    sql = """insert into wmbs_file_runlumi_map (file, run, lumi) 
                values ((select id from wmbs_file_details where lfn = :lfn), :run, :lumi)"""
                
    def getBinds(self, files=None, run=0, lumi=0):
        # Can't use self.dbi.buildbinds here...
        binds = {}
        if type(files) == type('string'):
            binds = {'lfn': files, 
                     'run': run, 
                     'lumi':lumi} 
        elif type(files) == type([]):
        # files is a list of tuples containing lfn, size, events, run and lumi
            binds = []
            for f in files:
                binds.append({'lfn': f[0], 
                              'run': f[3], 
                              'lumi':f[4]})
        return binds
    
    def execute(self, files=None, run=0, lumi=0, conn = None, transaction = False):
        binds = self.getBinds(files, run, lumi)
        
        self.logger.debug('AddRunLumi binds: %s' % binds)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)