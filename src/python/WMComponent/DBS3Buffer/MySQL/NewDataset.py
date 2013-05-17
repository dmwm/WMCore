#!/usr/bin/env python
"""
_NewDataset_

MySQL implementation of DBSBuffer.NewDataset
"""

from WMCore.Database.DBFormatter import DBFormatter

class NewDataset(DBFormatter):
    """
    _NewDataset_

    Check for new dataset existence and then
    insert it
    """

    sql = """INSERT IGNORE INTO dbsbuffer_dataset (path, processing_ver, acquisition_era, valid_status, global_tag, parent)
               VALUES (:path, :processing_ver, :acquisition_era, :valid_status, :global_tag, :parent)"""

    def execute(self, datasetPath, acquisitionEra = None, processingVer = None, validStatus = None,
                globalTag = None, parent = None, conn = None, transaction = False):
        """
        _execute_

        Insert new dataset
        """
        binds = {"path": datasetPath, 'acquisition_era': acquisitionEra,
                 'processing_ver': processingVer, 'valid_status': validStatus,
                 'global_tag': globalTag, 'parent': parent}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)

        return
