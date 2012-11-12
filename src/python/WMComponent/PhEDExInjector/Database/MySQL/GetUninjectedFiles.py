#!/usr/bin/env python
"""
_GetUninjectedFiles_

Retrieve a list of files that have been injected into DBS but not PhEDEx.
Format the output so that it can easily be injected into PhEDEx.

The location of a file for PhEDEx can be overridden by the spec with the field
overrides.phedexInjectionSite = 'T0_CH_CERN_Buffer'
"""

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

class GetUninjectedFiles(DBFormatter):
    sql = """SELECT dbsbuffer_file.lfn AS lfn,
                    dbsbuffer_file.filesize AS filesize,
                    dbsbuffer_block.blockname AS blockname,
                    dbsbuffer_dataset.path AS dataset,
                    dbsbuffer_dataset.custodial_site AS custodial_site,
                    dbsbuffer_location.se_name AS location,
                    dbsbuffer_file_checksums.cksum as cksum,
                    dbsbuffer_checksum_type.type as cktype,
                    dbsbuffer_workflow.spec AS spec
                    FROM dbsbuffer_file
               INNER JOIN dbsbuffer_file_checksums ON
                 dbsbuffer_file.id = dbsbuffer_file_checksums.fileid
               INNER JOIN dbsbuffer_checksum_type ON
                 dbsbuffer_file_checksums.typeid = dbsbuffer_checksum_type.id
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
               LEFT OUTER JOIN dbsbuffer_workflow ON
                 dbsbuffer_workflow.id = dbsbuffer_file.workflow
             WHERE dbsbuffer_file.in_phedex = 0 AND
               (dbsbuffer_file.status = 'LOCAL' OR dbsbuffer_file.status = 'GLOBAL')"""

    def loadOverride(self, specPath):
        """
        _loadOverride_

        Loads the spec (if not in the cache)
        and gets the location where the files should be injected to.
        If not possible to load the spec, raise an exception.
        """
        helper = WMWorkloadHelper()
        helper.load(specPath)
        overrideSite = helper.getPhEDExInjectionOverride()
        self.specCache[specPath] = overrideSite
        return overrideSite

    def format(self, result):
        """
        _format_

        Format the query results into something that resembles the XML format
        PhEDEx expects for injection:

        {"location1":
          {"dataset1":
            {"block1": {"is-open": "y", "files":
              [{"lfn": "lfn1", "size": 10, "checksum": {"cksum": 4321}},
               {"lfn": "lfn2", "size": 20, "checksum": {"cksum": 4321}]}}}}

        In order to do this, we have to graph the checksum.

        Change the site from the stored location in the DB if the spec specifies
        an override. If the spec is not accessible the file will not be injected
        and will be retried in another poll cycle. The error will be logged.
        """
        dictResult = DBFormatter.formatDict(self, result)
        self.specCache = {}
        formattedResult = {}
        for row in dictResult:
            overridden = False
            if row['spec'] is not None:
                specPath = row['spec']
                try:
                    location = self.loadOverride(specPath)
                    if location:
                        overridden = True
                except Exception, ex:
                    self.logger.error("Location could not be extracted")
                    self.logger.error("Error: %s" % str(ex))
                    continue
            if not overridden:
                if row['custodial_site'] != None:
                    location = row['custodial_site']
                elif not overridden:
                    location = row['location']


            if location not in formattedResult.keys():
                formattedResult[location] = {}

            locationDict = formattedResult[location]
            if row["dataset"] not in locationDict.keys():
                locationDict[row["dataset"]] = {}

            datasetDict = locationDict[row["dataset"]]
            if row["blockname"] not in datasetDict.keys():
                datasetDict[row["blockname"]] = {"is-open": "y",
                                                 "files": []}

            blockDict = datasetDict[row["blockname"]]
            for file in blockDict["files"]:
                if file["lfn"] == row["lfn"]:
                    file["checksum"][row["cktype"]] = row["cksum"]
                    break
            else:
                cksumDict = {row["cktype"]: row["cksum"]}
                blockDict["files"].append({"lfn": row["lfn"],
                                           "size": row["filesize"],
                                           "checksum": cksumDict})

        return formattedResult

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.format(result)
