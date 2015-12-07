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
        self.sql += "requestor_base_priority ) VALUES (:username, :email, :dnname, :priority)"
        binds = {"username":username, "email": email, "dnname": dnName, "priority": int(priority)}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = trans)
        return self.format(result)
