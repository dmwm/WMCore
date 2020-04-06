#!/usr/bin/env python
"""
_MarkDatasetSubscribed_

MySQL implementation of PhEDExInjector.Database.MarkDatasetSubscribed
"""
from __future__ import division

from WMCore.Database.DBFormatter import DBFormatter


class MarkDatasetSubscribed(DBFormatter):
    """
    _MarkDatasetSubscribed_

    Marks the given dataset subscription as subscribed in the database
    """

    sql = "UPDATE dbsbuffer_dataset_subscription SET subscribed = 1 WHERE id = :id"

    def execute(self, subIds, conn=None, transaction=False):
        binds = []
        for subId in subIds:
            binds.append({"id": subId})
        self.dbi.processData(self.sql, binds,
                             conn=conn, transaction=transaction)
        return
