#!/usr/bin/env python

"""
This code should load the necessary information regarding
dataset-algo combinations from the DBSBuffer.

Oracle version

"""




from WMComponent.DBS3Buffer.MySQL.FindDASToUpload import FindDASToUpload as MySQLFindDASToUpload


class FindDASToUpload(MySQLFindDASToUpload):
    """
    Find Uploadable DAS

    """

    sql = """SELECT das.dataset_id AS dataset, ds.Path as Path, das.algo_id as Algo, das.in_dbs as in_dbs,
               das.id AS das_id,
               ds.acquisition_era AS AcquisitionEra,
               ds.processing_era AS ProcessingEra,
               da.app_name AS ApplicationName,
               da.app_ver AS ApplicationVersion,
               da.app_fam AS ApplicationFamily,
               da.PSet_Hash as PSetHash,
               da.config_content as PSetContent,
               da.in_dbs AS algo_in_dbs
             FROM dbsbuffer_algo_dataset_assoc das
             INNER JOIN dbsbuffer_dataset ds ON ds.id = das.dataset_id
             INNER JOIN dbsbuffer_algo da ON da.id = das.algo_id
             WHERE EXISTS (SELECT id FROM dbsbuffer_file dbsfile
                             WHERE dbsfile.dataset_algo = das.id
                             AND dbsfile.status = :status
                             AND NOT EXISTS (SELECT id FROM dbsbuffer_file dbf2
                                              INNER JOIN dbsbuffer_file_parent dbfp ON dbf2.id = dbfp.parent
                                              WHERE dbf2.status = 'NOTUPLOADED'
                                              AND dbfp.child = dbsfile.id))"""
