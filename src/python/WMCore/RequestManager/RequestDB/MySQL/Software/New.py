#!/usr/bin/env python
"""
_Software.New_

Record a new Software version available for requests

"""




from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    """
    _New_

    Insert a new software version into the DB

    """
    sql = """INSERT INTO reqmgr_software (software_name, scram_arch)
               VALUES (:software_name, :scram_arch)
    """

    def execute(self, softwareNames, scramArch = None, conn = None, trans = False):
        """
        _execute_

        Adds scram_arch and software_name to DB
        Expects a scram_arch and a list of associated software versions.
        """
        if len(softwareNames) < 1:
            return

        binds = []
        for version in softwareNames:
            binds.append({'scram_arch': scramArch,
                          'software_name': version})
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return
