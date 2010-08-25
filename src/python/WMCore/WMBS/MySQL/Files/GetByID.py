"""
MySQL implementation of File.Get
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetByID(DBFormatter):
    sql = """SELECT id, lfn, size, events, cksum, first_event, last_event, merged
             FROM wmbs_file_details WHERE id = :fileid"""

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
        formattedResult["first_event"] = int(formattedResult["first_event"])
        formattedResult["last_event"] = int(formattedResult["last_event"])
        formattedResult["merged"] = bool(int(formattedResult["merged"]))

        if "size" in formattedResult.keys():
            formattedResult["size"] = int(formattedResult["size"])
        else:
            # The size column is named "filesize" in Oracle as size is
            # as reserved word.  We'll handle this here to make things
            # easier in the Oracle version of this object.
            formattedResult["size"] = int(formattedResult["filesize"])
            del formattedResult["filesize"]
            
        return formattedResult
    
    def execute(self, file = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"fileid": file}, 
                         conn = conn, transaction = transaction)
        return self.formatDict(result)
