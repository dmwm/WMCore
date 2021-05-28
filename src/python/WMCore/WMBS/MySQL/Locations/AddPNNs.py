#!/usr/bin/env python
"""
Adds PhEDEx Node Names into the WMBS database.
"""
from __future__ import division

from builtins import str, bytes

from WMCore.Database.DBFormatter import DBFormatter


class AddPNNs(DBFormatter):

    sql = "INSERT IGNORE INTO wmbs_pnns (pnn) VALUES (:pnn)"

    def execute(self, pnns, conn=None, transaction=False):
        """
        Adds either a single or a list of PNNs into WMBS
        """
        if isinstance(pnns, (str, bytes)):
            pnns = [pnns]

        binds = []
        for pnn in pnns:
            binds.append({'pnn': pnn})

        self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)

        return
