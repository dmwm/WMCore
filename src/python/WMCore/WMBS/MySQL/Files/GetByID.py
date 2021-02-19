"""
MySQL implementation of File.Get
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetByID(DBFormatter):
    sql = """SELECT id, lfn, filesize, events, first_event, merged
             FROM wmbs_file_details WHERE id = :fileid"""

    def formatOneDict(self, result):
        """
        _formatOneDict_

        Cast the integer attributes of the file object to integers as
        formatDict() will turn everything into a string.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"] = int(formattedResult["id"])
        formattedResult["events"] = int(formattedResult["events"])
        formattedResult["first_event"] = int(formattedResult["first_event"])
        formattedResult["merged"] = bool(int(formattedResult["merged"]))

        if "size" in formattedResult:
            formattedResult["size"] = int(formattedResult["size"])
        else:
            # The size column is named "filesize" in Oracle as size is
            # as reserved word.  We'll handle this here to make things
            # easier in the Oracle version of this object.
            formattedResult["size"] = int(formattedResult["filesize"])
            del formattedResult["filesize"]

        return formattedResult

    def formatBulkDict(self, result):
        """
        _formatBulkDict_

        Formats a whole list of dictionaries
        """

        formattedResult = {}
        listOfDicts     = DBFormatter.formatDict(self, result)

        for entry in listOfDicts:
            tmpDict = {}
            tmpDict["id"]          = int(entry["id"])
            tmpDict["lfn"]         = entry["lfn"]
            tmpDict["events"]      = int(entry["events"])
            tmpDict["first_event"] = int(entry["first_event"])
            if "size" in entry:
                tmpDict["size"]    = int(entry["size"])
            else:
                tmpDict["size"]    = int(entry["filesize"])
                del entry["filesize"]
            formattedResult[tmpDict['id']] = tmpDict

        return formattedResult

    def execute(self, file = None, conn = None, transaction = False):

        #Making some modifications to allow it to load a whole list of files
        #This DAO object should be called directly, not through WMBSFile
        if isinstance(file, list):
            #Then we have a list of the form [fileid, fileid, etc.]
            if len(file) == 0:
                #Ignore empty lists
                return {}
            binds = []
            for id in file:
                binds.append({'fileid': id})

            result = self.dbi.processData(self.sql, binds,
                                          conn = conn, transaction = transaction)
            return self.formatBulkDict(result)
        else:
            #We only have one file ID
            binds = {"fileid": file}
            result = self.dbi.processData(self.sql, binds,
                                          conn = conn, transaction = transaction)
            return self.formatOneDict(result)
