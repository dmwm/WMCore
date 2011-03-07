#!/usr/bin/env python

"""
Load the DAS info from the DAS ID

"""

from WMComponent.DBSUpload.Database.MySQL.LoadInfoFromDAS import LoadInfoFromDAS as MySQLLoadInfoFromDAS

class LoadInfoFromDAS(MySQLLoadInfoFromDAS):
    """
    Oracle version


    """


    sql = """SELECT das1.dataset_id AS dataset, ds1.Path as Path, das1.algo_id as Algo, das1.in_dbs as in_dbs,
               das1.id AS das_id,
               da1.app_name AS ApplicationName, 
               da1.app_ver AS ApplicationVersion, 
               da1.app_fam AS ApplicationFamily, 
               da1.PSet_Hash as PSetHash,
               da1.Config_Content as PSetContent,
               da1.in_dbs AS algo_in_dbs,
               ds1.valid_status AS valid_status
             FROM dbsbuffer_algo_dataset_assoc das1
             INNER JOIN dbsbuffer_algo da1 ON da1.id = das1.algo_id
             INNER JOIN dbsbuffer_dataset ds1 ON ds1.id = das1.dataset_id
             WHERE das1.id = :id
             """
