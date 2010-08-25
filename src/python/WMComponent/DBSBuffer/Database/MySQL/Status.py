#!/usr/bin/env python
"""
_Status_

MySQL implementation of DBSBuffer.Status
"""

__revision__ = "$Id: Status.py,v 1.1 2010/05/26 21:04:20 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Status(DBFormatter):
    """
    _Status_

    Retrieve information about the size and number of events contained in
    datasets in DBSBuffer.
    """
    sql = """SELECT dbsbuffer_dataset.path, dbsbuffer_algo.app_name, dbsbuffer_algo.app_ver,
                    SUM(events) AS events, SUM(filesize) AS filesize FROM dbsbuffer_file
               INNER JOIN dbsbuffer_algo_dataset_assoc ON
                 dbsbuffer_file.dataset_algo = dbsbuffer_algo_dataset_assoc.id
               INNER JOIN dbsbuffer_dataset ON
                 dbsbuffer_algo_dataset_assoc.dataset_id = dbsbuffer_dataset.id
               INNER JOIN dbsbuffer_algo ON
                 dbsbuffer_algo_dataset_assoc.algo_id = dbsbuffer_algo.id
               WHERE dbsbuffer_dataset.path != "bogus"
               GROUP BY dbsbuffer_dataset.path, dbsbuffer_algo.app_name, dbsbuffer_algo.app_ver"""
                             
    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        return self.formatDict(result)
