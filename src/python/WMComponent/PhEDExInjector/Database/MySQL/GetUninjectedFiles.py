#!/usr/bin/env python
"""
_GetUninjectedFiles_

Retrieve a list of files that have been injected into DBS but not PhEDEx.
Format the output so that it can easily be injected into PhEDEx.
"""

__revision__ = "$Id: GetUninjectedFiles.py,v 1.4 2009/12/10 16:06:02 mnorman Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetUninjectedFiles(DBFormatter):
    sql = """SELECT dbsbuffer_file.lfn AS lfn,
                    dbsbuffer_file.filesize AS filesize,
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

    findCksumSQL = """SELECT dbsbuffer_file_checksums.cksum as cksum,
                             dbsbuffer_checksum_type.type as cktype
                      FROM dbsbuffer_file_checksums
                      INNER JOIN dbsbuffer_checksum_type
                      ON dbsbuffer_file_checksums.typeid = dbsbuffer_checksum_type.id
                      WHERE dbsbuffer_file_checksum.fileid = (SELECT id FROM dbsbuffer_file WHERE lfn = :lfn)
    """

    def formatAndGrabChecksum(self, result, conn = None, transaction = False):
        """
        _formatDict_

        Format the query results into something that resembles the XML format
        PhEDEx expects for injection:

        {"location1":
          {"dataset1":
            {"block1": {"is-open": "y", "files":
              [{"lfn": "lfn1", "size": 10, "checksum": "cksum:1234"},
               {"lfn": "lfn2", "size": 20, "checksum": "cksum:4321"}]}}}}

        In order to do this, we have to graph the checksum
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

            #Now you have to get the checksum
            cksumString = self.getChecksum(lfn = row['lfn'])

            blockDict = datasetDict[row["blockname"]]
            blockDict["files"].append({"lfn": row["lfn"],
                                       "size": row["filesize"],
                                       "checksum": cksumString})

        return formattedResult
                 
    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.formatAndGrabChecksum(result, conn, transaction)


    def getChecksum(self, lfn, conn = None, transaction = False):
        """
        get a Checksum

        """

        result = self.dbi.processData(self.findCksumSQL, {'lfn': lfn}, conn = conn, transaction = transaction)

        dictResult = DBFormatter.formatDict(result)
        #This should be a list

        if not type(dictResult) == list:
            return None, None

        cksum = None
        adler = None
        for entry in dictResult:
            if entry['cktype'].lower() == 'cksum':
                #This is the priority one
                cksum = entry['cksum'] 
            elif entry['cktype'].lower() == 'adler32':
                adler = entry['cksum']
            #PhEDEX doesn't do anything else

        if not adler and not cksum:
            return None
        returnString = ''
        if adler:
            returnString.join('adler32:%s' %(adler))
        if cksum:
            returnString.join('cksum:%s' %(cksum))

        return returnString
