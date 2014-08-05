#!/usr/bin/env python
"""
_LoadFilesByWorkflow_

MySQL implementation of LoadFilesByWorkflow
"""




import logging

from WMComponent.DBS3Buffer.MySQL.LoadDBSFilesByDAS import LoadDBSFilesByDAS as MySQLLoadDBSFilesByDAS

class LoadFilesByWorkflow(MySQLLoadDBSFilesByDAS):
    fileInfoSQL = """SELECT files.id AS id, files.lfn AS lfn, files.filesize AS filesize,
                    files.events AS events,
                    files.status AS status,
                    dbsbuffer_algo.app_name AS app_name, dbsbuffer_algo.app_ver AS app_ver,
                    dbsbuffer_algo.app_fam AS app_fam, dbsbuffer_algo.pset_hash AS pset_hash,
                    dbsbuffer_algo.config_content, dbsbuffer_dataset.path AS dataset_path,
                    dbsbuffer_dataset.acquisition_era AS acquisition_era,
                    dbsbuffer_dataset.processing_ver AS processing_ver,
                    dbsbuffer_dataset.global_tag AS global_tag,
                    dbsbuffer_dataset.prep_id AS prep_id
             FROM dbsbuffer_file files
             INNER JOIN dbsbuffer_algo_dataset_assoc ON
               files.dataset_algo = dbsbuffer_algo_dataset_assoc.id
             INNER JOIN dbsbuffer_algo ON
               dbsbuffer_algo_dataset_assoc.algo_id = dbsbuffer_algo.id
             INNER JOIN dbsbuffer_dataset ON
               dbsbuffer_algo_dataset_assoc.dataset_id = dbsbuffer_dataset.id
             WHERE files.workflow = (SELECT id FROM dbsbuffer_workflow WHERE name = :workflow)
             ORDER BY files.id"""

    def execute(self, workflowName, conn = None, transaction = False):
        """
        This loads all files in a workflow.

        It has no defenses against loading files with unloaded parents.
        It depends on nobody but LoadDBSFilesByDAS putting files into a block
        """
        result   = self.dbi.processData(self.fileInfoSQL, {'workflow': workflowName},
                                        conn = conn, transaction = transaction)
        fileInfo = self.formatFileInfo(result)

        fileIDs  = [x['id'] for x in fileInfo]
        binds    = self.getBinds(fileIDs)

        if len(fileInfo) == 0:
            # Then we have no files for this workflow
            return []

        # Do locations
        result      = self.dbi.processData(self.getLocationSQL, binds, conn=conn, transaction=transaction)
        locInfo     = self.locInfo(result)
        fullResults = self.merge(fileInfo, locInfo)

        # Do checksums
        result      = self.dbi.processData(self.getChecksumSQL, binds, conn=conn, transaction=transaction)
        ckInfo      = self.ckInfo(result)
        fullResults = self.merge(fullResults, ckInfo)

        # Do runLumi
        result      = self.dbi.processData(self.getRunLumiSQL, binds, conn=conn, transaction=transaction)
        runInfo     = self.runInfo(result)
        fullResults = self.merge(fullResults, runInfo)

        # Do parents
        result      = self.dbi.processData(self.getParentLFNSQL, binds, conn=conn, transaction=transaction)
        parInfo     = self.parentInfo(result)
        fullResults = self.merge(fullResults, parInfo)

        return fullResults
