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
    sql = """SELECT DISTINCT dbsbuffer_algo_dataset_assoc.dataset_id AS dataset,
                             dbsbuffer_dataset.Path as Path, dbsbuffer_algo_dataset_assoc.algo_id as Algo,
                             dbsbuffer_algo_dataset_assoc.in_dbs as in_dbs,
                             dbsbuffer_algo_dataset_assoc.id AS das_id,
                             dbsbuffer_dataset.acquisition_era AS AcquisitionEra,
                             dbsbuffer_dataset.processing_ver AS ProcessingVer,
                             dbsbuffer_dataset.global_tag AS global_tag,
                             dbsbuffer_dataset.prep_id AS prep_id,
                             dbsbuffer_algo.app_name AS ApplicationName,
                             dbsbuffer_algo.app_ver AS ApplicationVersion,
                             dbsbuffer_algo.app_fam AS ApplicationFamily,
                             dbsbuffer_algo.PSet_Hash as PSetHash,
                             dbsbuffer_algo.config_content as PSetContent,
                             dbsbuffer_algo.in_dbs AS algo_in_dbs
             FROM dbsbuffer_algo_dataset_assoc
             INNER JOIN dbsbuffer_dataset ON
               dbsbuffer_dataset.id = dbsbuffer_algo_dataset_assoc.dataset_id
             INNER JOIN dbsbuffer_algo ON
               dbsbuffer_algo.id = dbsbuffer_algo_dataset_assoc.algo_id
             WHERE EXISTS (SELECT id FROM dbsbuffer_file dbsfile
                             WHERE dbsfile.dataset_algo = dbsbuffer_algo_dataset_assoc.id
                             AND dbsfile.status = :status
                             AND NOT EXISTS (SELECT id FROM dbsbuffer_file dbf2
                                              INNER JOIN dbsbuffer_file_parent dbfp ON dbf2.id = dbfp.parent
                                              WHERE dbf2.status = 'NOTUPLOADED'
                                              AND dbfp.child = dbsfile.id))
             """

    def makeDAS(self, results):
        ret=[]
        for r in results:
            entry={}
            entry['Path']=r['path']
            entry['DAS_ID'] = long(r['das_id'])
            if not r['algo'] == None:
                entry['Algo'] = int(r['algo'])
            else:
                entry['Algo'] = None
            if not r['algo_in_dbs'] == None:
                entry['AlgoInDBS'] = int(r['algo_in_dbs'])
            else:
                entry['AlgoInDBS'] = None
            if not r['in_dbs'] == None:
                entry['DASInDBS'] = int(r['in_dbs'])
            else:
                entry['DASInDBS'] = None
            path = r['path']
            entry['PrimaryDataset']     = path.split('/')[1]
            entry['ProcessedDataset']   = path.split('/')[2]
            entry['DataTier']           = path.split('/')[3]
            entry['ApplicationName']    = r['applicationname']
            entry['ApplicationVersion'] = r['applicationversion']
            entry['ApplicationFamily']  = r['applicationfamily']
            entry['PSetHash']           = r['psethash']
            entry['PSetContent']        = r['psetcontent']
            entry['Dataset']            = r['dataset']
            entry['AcquisitionEra']     = r['acquisitionera']
            if r["processingver"].count("-") == 1:
                (junk, entry["ProcessingVer"]) = r["processingver"].split("-v")
            else:
                entry['ProcessingVer']      = r['processingver']
                
            entry['GlobalTag']          = r['global_tag']
            entry['prep_id']          = r['prep_id']
            ret.append(entry)

        return ret

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"status": "NOTUPLOADED"},
                                      conn = conn, transaction = transaction)
        return self.makeDAS(self.formatDict(result))
