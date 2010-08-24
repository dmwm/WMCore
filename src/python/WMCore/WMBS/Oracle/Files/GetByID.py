"""
Oracle implementation of GetByID
"""

from WMCore.WMBS.MySQL.Files.GetByID import GetByID as GetByIDMySQL

class GetByID(GetByIDMySQL):
    sql = """SELECT id, lfn, filesize, events, cksum
             FROM wmbs_file_details WHERE id = :fileid"""

    def formatDict(self, result):
        """
        _formatDict_

        Override the formatDict() method so that we can change the name
        of the filesize column to just be size.
        """
        formattedResult = GetByIDMySQL.formatDict(self, result)
        formattedResult[0]["size"] = formattedResult[0]["filesize"]
        del formattedResult[0]["filesize"]

        return formattedResult
