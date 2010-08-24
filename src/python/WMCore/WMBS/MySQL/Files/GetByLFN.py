"""
MySQL implementation of File.Get
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetByLFN(DBFormatter):
    sql = """SELECT id, lfn, size, events, cksum from wmbs_file_details
             WHERE lfn = :lfn"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the integer attributes of the file object to integers as
        formatDict() will turn everything into a string.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"] = int(formattedResult["id"])
        formattedResult["events"] = int(formattedResult["events"])
        formattedResult["cksum"] = int(formattedResult["cksum"])

        if "size" in formattedResult.keys():
            formattedResult["size"] = int(formattedResult["size"])
        else:
            # The size column is named "filesize" in Oracle as size is
            # as reserved word.  We'll handle this here to make things
            # easier in the Oracle version of this object.
            formattedResult["size"] = int(formattedResult["filesize"])
            del formattedResult["filesize"]
        
        return formattedResult

    def execute(self, lfn = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"lfn": lfn},
                         conn = conn, transaction = transaction)
        return self.formatDict(result)



