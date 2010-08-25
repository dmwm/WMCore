"""
MySQL implementation of File.GetByLFN
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetByLFN(DBFormatter):
    sql = """SELECT id, lfn, size, events, cksum, first_event, last_event
             FROM wmbs_file_details WHERE lfn = :lfn"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the integer attributes of the file object to integers as
        formatDict() will turn everything into a string.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"] = int(formattedResult["id"])

        if formattedResult["events"] != None:
            formattedResult["events"] = int(formattedResult["events"])
        if formattedResult["cksum"] != None:
            formattedResult["cksum"] = int(formattedResult["cksum"])
        if formattedResult["first_event"] != None:
            formattedResult["first_event"] = int(formattedResult["first_event"])
        if formattedResult["last_event"] != None:
            formattedResult["last_event"] = int(formattedResult["last_event"])

        if "size" in formattedResult.keys():
            formattedResult["size"] = formattedResult["size"]
        else:
            # The size column is named "filesize" in Oracle as size is
            # as reserved word.  We'll handle this here to make things
            # easier in the Oracle version of this object.
            formattedResult["size"] = formattedResult["filesize"]
            del formattedResult["filesize"]

        if formattedResult["size"] != None:
            formattedResult["size"] = int(formattedResult["size"])
            
        return formattedResult

    def execute(self, lfn = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"lfn": lfn},
                         conn = conn, transaction = transaction)
        return self.formatDict(result)
