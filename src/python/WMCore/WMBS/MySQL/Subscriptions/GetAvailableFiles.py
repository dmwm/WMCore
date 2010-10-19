#!/usr/bin/env python
"""
_AvailableFiles_

MySQL implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetAvailableFiles(DBFormatter):
    sql = """SELECT wmbs_sub_files_available.file, wmbs_location.se_name
                    FROM wmbs_sub_files_available
               INNER JOIN wmbs_file_location ON
                 wmbs_sub_files_available.file = wmbs_file_location.file
               INNER JOIN wmbs_location ON
                 wmbs_file_location.location = wmbs_location.id
             WHERE wmbs_sub_files_available.subscription = :subscription"""

    def formatDict(self, results):
        """
        _formatDict_

        Cast the file column to an integer as the DBFormatter's formatDict()
        method turns everything into strings.  Also, fixup the results of the
        Oracle query by renaming 'fileid' to file.
        """
        formattedResults = DBFormatter.formatDict(self, results)

        for formattedResult in formattedResults:
            if "file" in formattedResult.keys():
                formattedResult["file"] = int(formattedResult["file"])
            else:
                formattedResult["file"] = int(formattedResult["fileid"])

        #Now the tricky part
        tempResults = {}
        for formattedResult in formattedResults:
            if formattedResult["file"] not in tempResults.keys():
                tempResults[formattedResult["file"]] = []
            if "se_name" in formattedResult.keys():
                tempResults[formattedResult["file"]].append(formattedResult["se_name"])

        finalResults = []
        for key in tempResults.keys():
            tmpDict = {"file": key}
            if not tempResults[key] == []:
                tmpDict['locations'] = tempResults[key]
            finalResults.append(tmpDict)

        return finalResults
           
    def execute(self, subscription, conn = None, transaction = False, returnCursor = False):
        if returnCursor:
            return self.dbi.processData(self.sql, {"subscription": subscription},
                                        conn = conn, transaction = transaction,
                                        returnCursor = returnCursor)
        
        results = self.dbi.processData(self.sql, {"subscription": subscription},
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
