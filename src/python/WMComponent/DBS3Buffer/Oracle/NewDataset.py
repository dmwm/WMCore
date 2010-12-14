#!/usr/bin/env python
"""
_NewDataset_

Oracle implementation of DBSBuffer.NewDataset
"""




from WMCore.Database.DBFormatter import DBFormatter

class NewDataset(DBFormatter):
    sql = """INSERT INTO dbsbuffer_dataset (path, processing_era, acquisition_era)
               SELECT :path, :processing_era, :acquisition_era FROM DUAL
               WHERE NOT EXISTS (SELECT * FROM dbsbuffer_dataset WHERE path = :path)"""

    def execute(self, datasetPath, conn = None, acquisitionEra = None,
                processingEra = None, transaction = False):
        binds = {"path": datasetPath, 'acquisition_era': acquisitionEra,
                 'processing_era': processingEra}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return 
