#!/usr/bin/env python

"""
Load the DAS info from the DAS ID

"""

from WMComponent.DBSUpload.Database.MySQL.FindDASToUpload import FindDASToUpload

class LoadInfoFromDAS(FindDASToUpload):
    """
    _LoadInfoFromDAS_
    
    Given a DAS ID load the dataset and algo information
    """


    sql = """SELECT DISTINCT das.dataset_id AS dataset, ds.Path as Path, das.algo_id as Algo, das.in_dbs as in_dbs,
               das.id AS das_id,
               da.app_name AS ApplicationName, 
               da.app_ver AS ApplicationVersion, 
               da.app_fam AS ApplicationFamily, 
               da.PSet_Hash as PSetHash,
               da.Config_Content as PSetContent,
               da.in_dbs AS algo_in_dbs,
               ds.valid_status AS valid_status,
               ds.global_tag AS global_tag
             FROM dbsbuffer_algo_dataset_assoc das
             INNER JOIN dbsbuffer_dataset ds ON ds.id = das.dataset_id
             INNER JOIN dbsbuffer_algo da ON da.id = das.algo_id
             WHERE das.id = :id
             """


    def execute(self, ids, conn=None, transaction = False):
        """
        _execute_

        Take a list of IDs, return info
        """
        binds  = []
        if not type(ids) == type([]):
            ids = list(ids)
        for id in ids:
            binds.append({'id': id})
            
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        
        return self.makeDAS(self.formatDict(result))
