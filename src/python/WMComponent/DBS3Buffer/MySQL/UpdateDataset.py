#!/usr/bin/env python
"""
_UpdateDataset_

MySQL implementation of DBS3Buffer.UpdateDataset

Created on May 15, 2013

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class UpdateDataset(DBFormatter):
    """
    _UpdateDataset_

    Update the information of a dataset record in the
    DBSBuffer database
    """

    sql = """UPDATE dbsbuffer_dataset
              SET processing_ver = :procVer, acquisition_era = :acqEra,
                  valid_status = :valid, global_tag = :globalTag,
                  parent = :parent, prep_id = :prep_id
              WHERE id = :id
           """
    def execute(self, datasetId,
                processingVer = None, acquisitionEra = None,
                validStatus = None, globalTag = None,
                parent = None, prep_id = None,
                conn = None, transaction = False):

        bindVars = {"procVer" : processingVer,
                    "acqEra" : acquisitionEra,
                    "valid" : validStatus,
                    "globalTag" : globalTag,
                    "parent" : parent,
                    "prep_id" : prep_id,
                    "id" : datasetId}
        self.dbi.processData(self.sql, bindVars, conn = conn,
                             transaction = transaction)

        return
