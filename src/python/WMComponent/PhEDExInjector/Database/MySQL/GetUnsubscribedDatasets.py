#!/usr/bin/env python
"""
_GetUnsubscribedDatasets_

"""

__revision__ = "$Id: GetUnsubscribedDatasets.py,v 1.1 2010/04/01 19:54:09 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetUnsubscribedDatasets(DBFormatter):
    sql = """SELECT DISTINCT dbsbuffer_dataset.path, dbsbuffer_location.se_name
               FROM dbsbuffer_dataset
               INNER JOIN dbsbuffer_algo_dataset_assoc ON
                 dbsbuffer_dataset.id = dbsbuffer_algo_dataset_assoc.dataset_id
               INNER JOIN dbsbuffer_file ON
                 dbsbuffer_algo_dataset_assoc.id = dbsbuffer_file.dataset_algo
               INNER JOIN dbsbuffer_file_location ON
                 dbsbuffer_file.id = dbsbuffer_file_location.filename
               INNER JOIN dbsbuffer_location ON
                 dbsbuffer_file_location.location = dbsbuffer_location.id
             WHERE dbsbuffer_dataset.subscribed = 0"""    

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
