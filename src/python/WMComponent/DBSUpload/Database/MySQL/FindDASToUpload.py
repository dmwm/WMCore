#!/usr/bin/env python

"""
This code should load the necessary information regarding
dataset-algo combinations from the DBSBuffer.

"""
from WMCore.Database.DBFormatter import DBFormatter


class FindDASToUpload(DBFormatter):
    """
    Find Uploadable DAS

    """
    sql = """SELECT das.dataset_id AS dataset,
                    ds.Path as Path,
                    das.algo_id as Algo,
                    das.in_dbs as in_dbs,
                    das.id AS das_id,
                    ds.valid_status AS valid_status,
                    ds.global_tag AS global_tag,
                    ds.parent AS parent
             FROM dbsbuffer_algo_dataset_assoc das
             INNER JOIN dbsbuffer_dataset ds ON
               ds.id = das.dataset_id
             INNER JOIN dbsbuffer_file dbsfile ON
               dbsfile.dataset_algo = das.id AND
               dbsfile.status = 'NOTUPLOADED'
             WHERE UPPER(ds.Path) NOT LIKE 'BOGUS'
             GROUP BY das.dataset_id,
                      ds.Path,
                      das.algo_id,
                      das.in_dbs,
                      das.id,
                      ds.valid_status,
                      ds.global_tag,
                      ds.parent
             """

    def makeDAS(self, results):
        ret=[]
        for r in results:
            if r == {}:
                continue
            entry={}

            entry['DAS_ID'] = long(r['das_id'])

            if not r['algo'] == None:
                entry['Algo'] = int(r['algo'])
            else:
                entry['Algo'] = None

            if not r['in_dbs'] == None:
                entry['DASInDBS'] = int(r['in_dbs'])
            else:
                entry['DASInDBS'] = None

            # insert for upstream users
            entry['AlgoInDBS'] = None

            path = r['path']
            entry['Path']               = r['path']
            entry['PrimaryDataset']     = path.split('/')[1]
            entry['ProcessedDataset']   = path.split('/')[2]
            entry['DataTier']           = path.split('/')[3]
            entry['Dataset']            = r['dataset']
            entry['ValidStatus']        = r['valid_status']
            entry['GlobalTag']          = r['global_tag']
            entry['Parent']             = r['parent']

            ret.append(entry)

        return ret


    def execute(self, conn=None, transaction = False):

        result = self.dbi.processData(self.sql, binds = {}, conn = conn,
                                      transaction = transaction)

        return self.makeDAS(self.formatDict(result))
