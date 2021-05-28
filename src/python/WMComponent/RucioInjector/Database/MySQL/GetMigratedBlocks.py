#!/usr/bin/env python
"""
_GetMigratedBlocks_

"""

from __future__ import division

from WMCore.Database.DBFormatter import DBFormatter


class GetMigratedBlocks(DBFormatter):
    sql = """SELECT dbsbuffer_block.blockname AS blockname,
                    dbsbuffer_dataset.path AS dataset,
                    dbsbuffer_location.pnn AS location
                    FROM dbsbuffer_block
               INNER JOIN
                 (SELECT block_id, MIN(dbsbuffer_file.id) file_id FROM dbsbuffer_file
                  GROUP BY block_id) file_block_assoc ON
                 dbsbuffer_block.id = file_block_assoc.block_id
               INNER JOIN dbsbuffer_file ON
                 file_block_assoc.file_id = dbsbuffer_file.id
               INNER JOIN dbsbuffer_file_location ON
                 dbsbuffer_file.id = dbsbuffer_file_location.filename
               INNER JOIN dbsbuffer_location ON
                 dbsbuffer_file_location.location = dbsbuffer_location.id
               INNER JOIN dbsbuffer_algo_dataset_assoc ON
                 dbsbuffer_file.dataset_algo = dbsbuffer_algo_dataset_assoc.id
               INNER JOIN dbsbuffer_dataset ON
                 dbsbuffer_algo_dataset_assoc.dataset_id = dbsbuffer_dataset.id
             WHERE dbsbuffer_block.status = 'InDBS'
             AND NOT EXISTS (SELECT dbf.id FROM dbsbuffer_file dbf
                              WHERE dbf.block_id = dbsbuffer_block.id
                              AND dbf.in_phedex = 0)"""

    def formatDict(self, result):
        """
        _formatDict_

        Format the query results into something that resembles the XML format
        PhEDEx expects for injection:

        {"location1":
          {"dataset1":
            {"block1": {"is-open": "n", "files": []}}}}
        """
        dictResult = DBFormatter.formatDict(self, result)

        formattedResult = {}
        for row in dictResult:
            if row["location"] not in formattedResult:
                formattedResult[row["location"]] = {}

            locationDict = formattedResult[row["location"]]
            if row["dataset"] not in locationDict:
                locationDict[row["dataset"]] = {}

            datasetDict = locationDict[row["dataset"]]
            if row["blockname"] not in datasetDict:
                datasetDict[row["blockname"]] = {"is-open": "n",
                                                 "files": []}

        return formattedResult

    def execute(self, conn=None, transaction=False):
        result = self.dbi.processData(self.sql, conn=conn,
                                      transaction=transaction)
        return self.formatDict(result)
