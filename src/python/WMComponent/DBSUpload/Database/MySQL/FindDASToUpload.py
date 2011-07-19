#!/usr/bin/env python

"""
This code should load the necessary information regarding
dataset-algo combinations from the DBSBuffer.

"""
import logging
from WMCore.Database.DBFormatter import DBFormatter


class FindDASToUpload(DBFormatter):
    """
    Find Uploadable DAS

    """

    querySQL = """SELECT DISTINCT dbsfile.dataset_algo AS dasid FROM dbsbuffer_file dbsfile
                     WHERE dbsfile.status = 'NOTUPLOADED'"""


    sql = """SELECT das.dataset_id AS dataset, ds.Path as Path, das.algo_id as Algo, das.in_dbs as in_dbs,
               das.id AS das_id,
               ds.valid_status AS valid_status,
               ds.global_tag AS global_tag,
               ds.parent AS parent
             FROM dbsbuffer_algo_dataset_assoc das
             INNER JOIN dbsbuffer_dataset ds ON ds.id = das.dataset_id
             WHERE das.id = :das
             AND UPPER(ds.Path) NOT LIKE 'BOGUS'
             """


    def makeDAS(self, results):
        ret=[]
        for r in results:
            if r == {}:
                continue
            entry={}
            entry['Path']=r['path']
            entry['DAS_ID'] = long(r['das_id'])
            if not r['algo'] == None:
                entry['Algo'] = int(r['algo'])
            else:
                entry['Algo'] = None
            if not r['in_dbs'] == None:
                entry['DASInDBS'] = int(r['in_dbs'])
            else:
                entry['DASInDBS'] = None
            path = r['path']
            entry['PrimaryDataset']     = path.split('/')[1]
            entry['ProcessedDataset']   = path.split('/')[2]
            entry['DataTier']           = path.split('/')[3]
            entry['Dataset']            = r['dataset']
            entry['ValidStatus']        = r['valid_status']
            entry['GlobalTag']          = r.get('global_tag', '')
            entry['Parent']             = r.get('parent', '')
            ret.append(entry)

        return ret


    def execute(self, conn=None, transaction = False):

        binds  = {}
        query  = self.dbi.processData(self.querySQL, binds,
                                      conn = conn, transaction = transaction)
        qDict  = self.formatDict(query)
        idList = []
        for i in qDict:
            idList.append({'das': i['dasid']})

        if len(idList) < 1:
            return []

        result = self.dbi.processData(self.sql, idList, 
                         conn = conn, transaction = transaction)
        
        return self.makeDAS(self.formatDict(result))
