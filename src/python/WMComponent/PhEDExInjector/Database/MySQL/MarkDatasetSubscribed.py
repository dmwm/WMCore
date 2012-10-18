#!/usr/bin/env python
"""
_MarkDatasetSubscribed_

MySQL implementation of PhEDExInjector.Database.MarkDatasetSubscribed
"""

from WMCore.Database.DBFormatter import DBFormatter

class MarkDatasetSubscribed(DBFormatter):
    """
    _MarkDatasetSubscribed_

    Marks the given dataset as subscribed in the database,
    this can be either setting the state to 1 or 2,
    depending on the operation mode and type of subscription made.
    """

    sql = "UPDATE dbsbuffer_dataset SET subscribed = :subscribed WHERE path = :path"

    def execute(self, datasetPath, subscribed = 1, conn = None, transaction = False):
        self.dbi.processData(self.sql, binds = {"path": datasetPath,
                                                "subscribed" : subscribed},
                             conn = conn, transaction = transaction)
        return
