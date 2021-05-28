#!/usr/bin/env python
"""
_AvailableFiles_

MySQL implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""

from WMCore.Database.DBFormatter import DBFormatter


class GetAvailableFiles(DBFormatter):
    sql = """SELECT wsfa.fileid, wpnn.pnn FROM wmbs_sub_files_available wsfa
               INNER JOIN wmbs_file_location wfl ON wsfa.fileid = wfl.fileid
               INNER JOIN wmbs_pnns wpnn ON wpnn.id = wfl.pnn
             WHERE wsfa.subscription = :subscription"""

    def formatDict(self, results):
        """
        _formatDict_

        Cast the file column to an integer as the DBFormatter's formatDict()
        method turns everything into strings.  Also, fixup the results of the
        Oracle query by renaming 'fileid' to file.
        """
        formattedResults = DBFormatter.formatDict(self, results)

        for formattedResult in formattedResults:
            if "file" in formattedResult:
                formattedResult["file"] = int(formattedResult["file"])
            else:
                formattedResult["file"] = int(formattedResult["fileid"])

        # Now the tricky part
        tempResults = {}
        for formattedResult in formattedResults:
            fileID = formattedResult['file']
            if fileID not in tempResults:
                tempResults[fileID] = []
            if "pnn" in formattedResult:
                if not formattedResult['pnn'] in tempResults[fileID]:
                    tempResults[fileID].append(formattedResult["pnn"])

        finalResults = []
        for key in tempResults:
            tmpDict = {"file": key}
            if not tempResults[key] == []:
                tmpDict['locations'] = tempResults[key]
            finalResults.append(tmpDict)

        return finalResults

    def execute(self, subscription, conn=None, transaction=False, returnCursor=False):
        if returnCursor:
            return self.dbi.processData(self.sql, {"subscription": subscription},
                                        conn=conn, transaction=transaction,
                                        returnCursor=returnCursor)

        results = self.dbi.processData(self.sql, {"subscription": subscription},
                                       conn=conn, transaction=transaction)
        return self.formatDict(results)
