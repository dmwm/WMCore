#!/usr/bin/env python
"""
_AlgoDatasetAssoc_

MySQL implementation of DBSBuffer.AlgoDatasetAssoc
"""




from WMCore.Database.DBFormatter import DBFormatter

class AlgoDatasetAssoc(DBFormatter):
    """
    _AlgoDatasetAssoc_

    Associtate an algorithm to a dataset and return the ID of the assocation.
    """
    sql = """INSERT INTO dbsbuffer_algo_dataset_assoc (algo_id, dataset_id)
               SELECT (SELECT id FROM dbsbuffer_algo WHERE app_name = :app_name AND
                         app_ver = :app_ver AND app_fam = :app_fam AND
                         pset_hash = :pset_hash) AS algo_id,
                      (SELECT id FROM dbsbuffer_dataset WHERE path = :path) AS dataset_id
               FROM DUAL WHERE NOT EXISTS
                 (SELECT * FROM dbsbuffer_algo_dataset_assoc WHERE algo_id =
                   (SELECT id FROM dbsbuffer_algo WHERE app_name = :app_name AND
                      app_ver = :app_ver AND app_fam = :app_fam AND
                      pset_hash = :pset_hash) AND dataset_id =
                   (SELECT id FROM dbsbuffer_dataset WHERE path = :path))"""

    idsql = """SELECT id FROM dbsbuffer_algo_dataset_assoc WHERE algo_id =
                 (SELECT id FROM dbsbuffer_algo WHERE app_name = :app_name AND
                    app_ver = :app_ver AND app_fam = :app_fam AND
                    pset_hash = :pset_hash) AND dataset_id =
                 (SELECT id FROM dbsbuffer_dataset WHERE path = :path)"""

    def execute(self, appName = None, appVer = None, appFam = None,
                psetHash = None, datasetPath = None, conn = None,
                transaction = False):
        binds = {"app_name": appName, "app_ver": appVer, "app_fam": appFam,
                 "pset_hash": psetHash, "path": datasetPath}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)

        result = self.dbi.processData(self.idsql, binds, conn = conn,
                                      transaction = transaction)
        formattedResult = self.formatDict(result)
        if len(formattedResult) == 1:
            return formattedResult[0]["id"]

        return None
