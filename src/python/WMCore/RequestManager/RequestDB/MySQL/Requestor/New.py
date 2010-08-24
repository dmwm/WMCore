#!/usr/bin/env python
"""
_Requestor.New_


"""






from WMCore.Database.DBFormatter import DBFormatter


class New(DBFormatter):
    """
    _New_

    Add a new requestor

    """

    def execute(self, username, email,
                dnName = None, priority = 0, conn = None, trans = False):


        self.sql = "INSERT INTO reqmgr_requestor (requestor_hn_name, "
        self.sql += "contact_email, requestor_dn_name,"
        self.sql += "requestor_base_priority ) VALUES ("

        self.sql += "\'%s\', " % username
        self.sql += "\'%s\', " % email
        self.sql += "\'%s\', " % dnName
        self.sql += " %s)" % priority
        result = self.dbi.processData(self.sql,
                         conn = conn, transaction = trans)
        return self.format(result)


