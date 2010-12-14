#!/usr/bin/env python
"""
_NewWorkflow_

Oracle implementation of NewWorkflow
"""

from WMCore.WMBS.MySQL.Users.New import New as NewUserMySQL

class New(NewUserMySQL):
    sql = """insert into wmbs_users (id, cert_dn, name_hn)
             values (wmbs_users_SEQ.nextval, :dn, :hn)"""
    

