#!/usr/bin/env python
"""
_SetLocationForWorkQueue_

MySQL implementation of Files.SetLocationForWorkQueue

For WorkQueue only
"""

from WMCore.Database.DBFormatter import DBFormatter

class SetLocationForWorkQueue(DBFormatter):
    """
    _SetLocationForWorkQueue_

    Set the location for a file, deleting all previous references
    and attaching the current references
    """

    deleteSQL = """DELETE FROM wmbs_file_location 
                     WHERE fileid = (SELECT wfd.id FROM wmbs_file_details wfd WHERE wfd.lfn = :lfn)"""

    insertSQL = """INSERT INTO wmbs_file_location (fileid, location) 
                     SELECT wmbs_file_details.id, wmbs_location.id
                       FROM wmbs_location, wmbs_file_details
                       WHERE wmbs_location.se_name = :location
                       AND wmbs_file_details.lfn = :lfn"""


    def execute(self, lfns, locations, conn = None, transaction = None):
        """
        First, delete all file_location references with that lfn.
        Then, insert the new ones.
        """
        binds = []
        for lfn in lfns:
            binds.append({'lfn': lfn})

        self.dbi.processData(self.deleteSQL, binds, conn = conn,
                             transaction = transaction)

        self.dbi.processData(self.insertSQL, locations, conn = conn,
                             transaction = transaction)
        return


