#!/usr/bin/env python
"""
_Software.List_

List known software versions with ids

"""




from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    _List_

    Get a map of software name to id from the DB

    """
    def execute(self, conn = None, trans = False):
        """
        _execute_

        Get mapping of software name to ids

        """
        self.sql = "SELECT software_name, software_id, scram_arch FROM reqmgr_software"

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)

        formattedDict = self.formatDict(result)
        formattedResult = {}
        for entry in formattedDict:
            scramArch = entry['scram_arch']
            version = entry['software_name']
            if not scramArch in formattedResult.keys():
                formattedResult[scramArch] = []
            formattedResult[scramArch].append(version)

        return formattedResult
