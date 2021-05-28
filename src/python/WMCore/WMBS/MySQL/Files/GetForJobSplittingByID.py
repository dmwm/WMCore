#!/usr/bin/env python

"""
_GetForJobSplittingByID_

MySQL implementation of File.GetForJobSplittingByID
"""

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.MySQL.Files.GetByID import GetByID


class GetForJobSplittingByID(GetByID):
    sql = """SELECT id, lfn, filesize, events, first_event, merged, MIN(run) AS minrun
             FROM wmbs_file_details wfd
             LEFT OUTER JOIN wmbs_file_runlumi_map wfr ON wfr.fileid = wfd.id
             WHERE id = :fileid"""

    def formatBulkDict(self, result):
        """
        _formatBulkDict_

        Formats a whole list of dictionaries
        """

        formattedResult = {}
        listOfDicts = DBFormatter.formatDict(self, result)

        for entry in listOfDicts:
            tmpDict = {}
            tmpDict["id"] = int(entry["id"])
            tmpDict["lfn"] = entry["lfn"]
            tmpDict["events"] = int(entry["events"])
            tmpDict["first_event"] = int(entry["first_event"])
            tmpDict['minrun'] = entry.get('minrun', None)
            if "size" in entry:
                tmpDict["size"] = int(entry["size"])
            else:
                tmpDict["size"] = int(entry["filesize"])
                del entry["filesize"]
            formattedResult[tmpDict['id']] = tmpDict

        return formattedResult
