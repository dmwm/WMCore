#!/usr/bin/env python
"""
_GetByID_

MySQL implementation of DBSBufferFiles.GetByID
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetByID(DBFormatter):
    sql = """SELECT files.id AS id, files.lfn AS lfn, files.filesize AS filesize,
                    files.events AS events,
                    files.status AS status,
                    dbsbuffer_algo.app_name AS app_name, dbsbuffer_algo.app_ver AS app_ver,
                    dbsbuffer_algo.app_fam AS app_fam, dbsbuffer_algo.pset_hash AS pset_hash,
                    dbsbuffer_algo.config_content, dbsbuffer_dataset.path AS dataset_path
             FROM dbsbuffer_file files
             INNER JOIN dbsbuffer_algo_dataset_assoc ON
               files.dataset_algo = dbsbuffer_algo_dataset_assoc.id
             INNER JOIN dbsbuffer_algo ON
               dbsbuffer_algo_dataset_assoc.algo_id = dbsbuffer_algo.id
             INNER JOIN dbsbuffer_dataset ON
               dbsbuffer_algo_dataset_assoc.dataset_id = dbsbuffer_dataset.id
             WHERE files.id = :fileid"""

    def format(self, result):
        """
        _format_

        Some databases (Oracle) aren't case sensitive with respect to column
        names so we'll do some formatting so the column names are returned as
        expected.
        """
        resultDict = self.formatDict(result)[0]
        resultDict["appName"] = resultDict["app_name"]
        del resultDict["app_name"]

        resultDict["appVer"] = resultDict["app_ver"]
        del resultDict["app_ver"]

        resultDict["appFam"] = resultDict["app_fam"]
        del resultDict["app_fam"]

        resultDict["psetHash"] = resultDict["pset_hash"]
        del resultDict["pset_hash"]

        resultDict["configContent"] = resultDict["config_content"]
        del resultDict["config_content"]

        resultDict["datasetPath"] = resultDict["dataset_path"]
        del resultDict["dataset_path"]

        resultDict["size"] = resultDict["filesize"]
        del resultDict["filesize"]

        return resultDict

    def getBinds(self, files):
        binds = []
        files = self.dbi.makelist(files)
        for f in files:
            binds.append({'fileid': f})
        return binds

    def execute(self, files, conn = None, transaction = False):
        binds = self.getBinds(files)
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
