#!/usr/bin/env python
"""
_MarkDatasetSubscribed_

"""

__revision__ = "$Id: MarkDatasetSubscribed.py,v 1.1 2010/04/01 19:54:09 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class MarkDatasetSubscribed(DBFormatter):
    sql = "UPDATE dbsbuffer_dataset SET subscribed = 1 WHERE path = :path"

    def execute(self, datasetPath, conn = None, transaction = False):
        self.dbi.processData(self.sql, binds = {"path": datasetPath},
                             conn = conn, transaction = transaction)
        return
