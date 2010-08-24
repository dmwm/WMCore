"""
Oracle implementation of Jobs.New
"""

import time

from WMCore.WMBS.MySQL.Jobs.New import New as NewJobMySQL

class New(NewJobMySQL):
    sql = """insert into wmbs_job (id, jobgroup, name, last_update) 
              values (wmbs_job_SEQ.nextval, :jobgroup, :name,
              %d)""" % time.time()
