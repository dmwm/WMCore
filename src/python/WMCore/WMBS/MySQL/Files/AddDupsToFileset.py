#!/usr/bin/env python
"""
_AddDupsToFileset_

MySQL implementation of Files.AddDupsToFileset
"""

import time

from WMCore.Database.DBFormatter import DBFormatter


class AddDupsToFileset(DBFormatter):
    existSQL = """SELECT lfn FROM wmbs_file_details wfd
                    INNER JOIN wmbs_fileset_files wff ON wff.fileid = wfd.id
                    WHERE wff.fileset = :fileset
                    AND wfd.lfn = :lfn"""

    sql = """INSERT IGNORE INTO wmbs_fileset_files (fileid, fileset, insert_time)
               SELECT wmbs_file_details.id, :fileset, :insert_time
               FROM wmbs_file_details
               WHERE wmbs_file_details.lfn = :lfn AND NOT EXISTS
                 (SELECT lfn FROM wmbs_file_details
                    INNER JOIN wmbs_fileset_files ON
                      wmbs_file_details.id = wmbs_fileset_files.fileid
                    INNER JOIN wmbs_subscription ON
                      wmbs_fileset_files.fileset = wmbs_subscription.fileset
                    INNER JOIN wmbs_workflow ON
                      wmbs_subscription.workflow = wmbs_workflow.id
                    WHERE wmbs_file_details.lfn = :lfn AND
                          wmbs_workflow.name = :workflow)"""

    sqlAvail = """INSERT IGNORE INTO wmbs_sub_files_available (subscription, fileid)
                    SELECT wmbs_subscription.id AS subscription,
                           wmbs_file_details.id AS fileid FROM wmbs_subscription
                      INNER JOIN wmbs_file_details ON
                        wmbs_file_details.lfn = :lfn
                    WHERE wmbs_subscription.fileset = :fileset AND NOT EXISTS
                 (SELECT lfn FROM wmbs_file_details
                    INNER JOIN wmbs_fileset_files ON
                      wmbs_file_details.id = wmbs_fileset_files.fileid
                    INNER JOIN wmbs_subscription ON
                      wmbs_fileset_files.fileset = wmbs_subscription.fileset
                    INNER JOIN wmbs_workflow ON
                      wmbs_subscription.workflow = wmbs_workflow.id
                    WHERE wmbs_file_details.lfn = :lfn AND
                          wmbs_workflow.name = :workflow AND
                          wmbs_fileset_files.fileset != :fileset)"""

    def execute(self, file, fileset, workflow, conn=None, transaction=False):
        binds = []
        availBinds = []
        existBinds = []
        timestamp = int(time.time())
        for fileLFN in file:
            existBinds.append({'lfn': fileLFN, 'fileset': fileset})
        existResult = self.dbi.processData(self.existSQL, existBinds,
                                           conn=conn,
                                           transaction=transaction)
        existsLFNs = self.formatDict(existResult)
        for result in existsLFNs:
            file.remove(result.get('lfn'))

        if len(file) < 1:
            # We removed all the files
            # There's nothing more for us to do.
            return

        for fileLFN in file:
            binds.append({"lfn": fileLFN, "fileset": fileset,
                          "insert_time": timestamp, "workflow": workflow})
            availBinds.append({"lfn": fileLFN, "fileset": fileset,
                               "workflow": workflow})

        self.dbi.processData(self.sql, binds, conn=conn,
                             transaction=transaction)
        self.dbi.processData(self.sqlAvail, availBinds, conn=conn,
                             transaction=transaction)
        return
