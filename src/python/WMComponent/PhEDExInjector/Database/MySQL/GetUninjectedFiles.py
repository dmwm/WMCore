#!/usr/bin/env python
"""
_GetUninjectedFiles_

Retrieve a list of files that have been injected into DBS but not PhEDEx.
Format the output so that it can easily be injected into PhEDEx.
"""

__revision__ = "$Id: GetUninjectedFiles.py,v 1.2 2009/12/02 17:54:31 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetUninjectedFiles(DBFormatter):
    sql = """SELECT dbsbuffer_file.lfn AS lfn,
                    dbsbuffer_file.filesize AS filesize,
                    dbsbuffer_file.cksum AS checksum,
                    dbsbuffer_block.blockname AS blockname,
                    dbsbuffer_dataset.path AS dataset,
                    dbsbuffer_location.se_name AS location
                    FROM dbsbuffer_file
               INNER JOIN dbsbuffer_algo_dataset_assoc ON
                 dbsbuffer_file.dataset_algo = dbsbuffer_algo_dataset_assoc.id
               INNER JOIN dbsbuffer_dataset ON
                 dbsbuffer_algo_dataset_assoc.dataset_id = dbsbuffer_dataset.id
               INNER JOIN dbsbuffer_block ON
                 dbsbuffer_file.block_id = dbsbuffer_block.id
               INNER JOIN dbsbuffer_file_location ON
                 dbsbuffer_file.id = dbsbuffer_file_location.filename
               INNER JOIN dbsbuffer_location ON
                 dbsbuffer_file_location.location = dbsbuffer_location.id
             WHERE dbsbuffer_file.status = 'InDBS'"""    

    def formatDict(self, result):
        """
        _formatDict_

        Format the query results into something that resembles the XML format
        PhEDEx expects for injection:

        {"location1":
          {"dataset1":
            {"block1": {"is-open": "y", "files":
                                  [{"lfn": "lfn1", "size": 10, "checksum": "cksum1234"},
                                   {"lfn": "lfn2", "size": 20, "checksum": "cksum4321"}]}}}}
        """
        dictResult = DBFormatter.formatDict(self, result)

        formattedResult = {}
        for row in dictResult:
            if row["location"] not in formattedResult.keys():
                formattedResult[row["location"]] = {}

            locationDict = formattedResult[row["location"]]
            if row["dataset"] not in locationDict.keys():
                locationDict[row["dataset"]] = {}

            datasetDict = locationDict[row["dataset"]]
            if row["blockname"] not in datasetDict.keys():
                datasetDict[row["blockname"]] = {"is-open": "y",
                                                 "files": []}

            blockDict = datasetDict[row["blockname"]]
            blockDict["files"].append({"lfn": row["lfn"],
                                       "size": row["filesize"],
                                       "checksum": str(row["checksum"])})

        return formattedResult
                 
    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
