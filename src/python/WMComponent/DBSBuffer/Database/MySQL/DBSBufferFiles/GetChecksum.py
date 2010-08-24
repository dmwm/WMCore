#!/usr/bin/env python
"""
_GetChecksum_

MySQL implementation of DBSBufferFiles.GetChecksum
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetChecksum(DBFormatter):
    sql = """SELECT cst.type AS cktype, fcs.cksum AS cksum FROM
               dbsbuffer_file_checksums fcs INNER JOIN
               dbsbuffer_checksum_type cst
               ON fcs.typeid = cst.id
               WHERE fcs.fileid = :fileid"""

    def formatResult(self, result):
        """
        _formatResult_

        Turn the query results into a dictionary that is keyed by
        checksum type.
        """
        formattedResult = {}

        dictVersion = DBFormatter.formatDict(self, result)

        for resultRow in dictVersion:
            formattedResult[resultRow["cktype"]] = resultRow["cksum"]

        return formattedResult

    def execute(self, fileid, conn = None, transaction = False):
        """
        _execute_

        Retrieve checksum information for a file.  The result is returned in
        the form of a dict that is keyed by checksum type.
        """
        result = self.dbi.processData(self.sql, {"fileid": fileid}, conn = conn,
                                      transaction = transaction)
        return self.formatResult(result)
