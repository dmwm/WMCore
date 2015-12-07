#!/usr/bin/env python
"""
_DestroyAll_

"""

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    tableSql = "SELECT table_name FROM user_tables"
    seqSql = "SELECT sequence_name FROM user_sequences"
    trgSql = "SELECT trigger_name FROM user_triggers"

    def execute(self, conn = None, transaction = False):
        tbl = self.dbi.processData(self.tableSql, conn = conn, transaction = transaction)
        seq = self.dbi.processData(self.seqSql, conn = conn, transaction = transaction)
        trg = self.dbi.processData(self.trgSql, conn = conn, transaction = transaction)

        for table in self.formatDict(tbl):
            self.dbi.processData("DROP TABLE %s CASCASE CONSTRAINTS" % table["table_name"],
                                 conn = conn, transaction = transaction)

        for sequence in self.formatDict(seq):
            self.dbi.processData("DROP SEQUENCE %s" % sequence["sequence_name"],
                                 conn = conn, transaction = transaction)

        for trigger in self.formatDict(trg):
            self.dbi.processData("DROP TTRIGGER %s" % trigger["trigger_name"],
                                 conn = conn, transaction = transaction)
