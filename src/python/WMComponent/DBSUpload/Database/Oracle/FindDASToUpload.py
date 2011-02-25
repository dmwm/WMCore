#!/usr/bin/env python

"""
This code should load the necessary information regarding
dataset-algo combinations from the DBSBuffer.

Oracle version

"""




from WMComponent.DBSUpload.Database.MySQL.FindDASToUpload import FindDASToUpload as MySQLFindDASToUpload


class FindDASToUpload(MySQLFindDASToUpload):
    """
    Find Uploadable DAS

    """


    sql = """SELECT das1.dataset_id AS dataset, ds1.Path as Path, das1.algo_id as Algo, das1.in_dbs as in_dbs,
               das1.id AS das_id,
               da1.app_name AS ApplicationName, 
               da1.app_ver AS ApplicationVersion, 
               da1.app_fam AS ApplicationFamily, 
               da1.PSet_Hash as PSetHash,
               da1.Config_Content as PSetContent,
               da1.in_dbs AS algo_in_dbs
             FROM dbsbuffer_algo_dataset_assoc das1
             INNER JOIN dbsbuffer_algo da1 ON da1.id = das1.algo_id
             INNER JOIN dbsbuffer_dataset ds1 ON ds1.id = das1.dataset_id
             WHERE das1.id =
             (SELECT DISTINCT das.id
             FROM dbsbuffer_algo_dataset_assoc das
             INNER JOIN dbsbuffer_dataset ds ON ds.id = das.dataset_id
             INNER JOIN dbsbuffer_algo da ON da.id = das.algo_id
             WHERE EXISTS (SELECT id FROM dbsbuffer_file dbsfile
                            WHERE dbsfile.dataset_algo = das.id
                            AND dbsfile.status = 'NOTUPLOADED')
             AND NOT EXISTS (SELECT dbf2.id FROM dbsbuffer_file dbf2
                              INNER JOIN dbsbuffer_file_parent dbfp ON dbf2.id = dbfp.parent
                              INNER JOIN dbsbuffer_file dbf3 ON dbf3.id = dbfp.child
                              LEFT OUTER JOIN dbsbuffer_block dbb2 ON dbb2.id = dbf2.block_id
                              WHERE dbf3.dataset_algo = das.id
                              AND (dbf2.status = 'NOTUPLOADED' OR (dbb2.status != 'Closed'
                                                                   AND dbb2.status != 'InGlobalDBS'
                                                                   AND dbb2.status IS NOT NULL)) )
             AND UPPER(ds.Path) NOT LIKE 'BOGUS')
             """
