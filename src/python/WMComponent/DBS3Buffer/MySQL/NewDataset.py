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
    
    sql = """INSERT IGNORE INTO dbsbuffer_dataset (path, processing_era, acquisition_era)
               VALUES (:path, :processing_era, :acquisition_era)"""

    def execute(self, datasetPath, acquisitionEra = None, processingEra = None,
                conn = None, transaction = False):
        """
        _execute_

        Insert new dataset
        """
        binds = {"path": datasetPath, 'acquisition_era': acquisitionEra,
                 'processing_era': processingEra}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)

        return 
