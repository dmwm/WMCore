#!/usr/bin/env python
"""
_SetDrain_

MySQL implementation of Locations.SetDrain
"""

from WMCore.Database.DBFormatter import DBFormatter

class SetDrain(DBFormatter):
    """
    _SetDrain_

    Set the drain character in the database

    Note: Drains are stored in T/F format for Oracle compatibility
    Translated back and forth from python Boolean
    """
    
    sql = """UPDATE wmbs_location
             SET drain = :drain
             WHERE site_name = :location
             """

    def execute(self, siteName, drain = True, conn = None, transaction = False):
        if drain:
            character = 'T'
        else:
            character = 'F'
        binds = {"location": siteName, "drain": character}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
