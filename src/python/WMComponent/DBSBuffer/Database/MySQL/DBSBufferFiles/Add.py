#!/usr/bin/env python





"""
MySQL implementation of AddFile
"""
from WMCore.Database.DBFormatter import DBFormatter

class Add(DBFormatter):

    sql = """INSERT IGNORE INTO dbsbuffer_file(lfn, filesize, events, dataset_algo, status) 
                values (:lfn, :filesize, :events, :dataset_algo, :status)"""
                
    def getBinds(self, files = None, size = 0, events = 0, cksum = 0,
                 dataset_algo = 0, status = "NOTUPLOADED"):
        # Can't use self.dbi.buildbinds here...
        binds = {}
        if type(files) == type('string'):
            binds = {'lfn': files, 
                     'filesize': size, 
                     'events': events,
                     'dataset_algo': dataset_algo,
                     'status' : status}
        elif type(files) == type([]):
        # files is a list of tuples containing lfn, size, events, cksum, dataset, status
            binds = []
            for f in files:
                binds.append({'lfn': f[0], 
                              'filesize': f[1], 
                              'events': f[2],
				'dataset_algo': f[3],
				'status' : f[4]})
        return binds
    
    def execute(self, files = None, size = 0, events = 0, cksum = 0,
                datasetAlgo = 0, status = "NOTUPLOADED", conn = None,
                transaction = False):
        binds = self.getBinds(files, size, events, cksum, datasetAlgo, status)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return
