"""
Oracle implementation of GetFileByLFN
"""
from WMCore.WMBS.MySQL.Files.GetByLFN import GetByLFN as GetByLFNMySQL

class GetByLFN(GetByLFNMySQL):
    sql = """SELECT id, lfn, filesize, events, cksum
             FROM wmbs_file_details WHERE lfn = :lfn"""

    def formatDict(self, result):
        """
        _formatDict_

        Override the formatDict() method so that we can change the name
        of the filesize column to just be size.
        """
        formattedResult = GetByLFNMySQL.formatDict(self, result)
        formattedResult[0]["size"] = formattedResult[0]["filesize"]
        del formattedResult[0]["filesize"]

        return formattedResult
