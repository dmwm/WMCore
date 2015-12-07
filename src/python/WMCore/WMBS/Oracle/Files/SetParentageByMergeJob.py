#!/usr/bin/env python
"""
Oracle implementation of File.SetParentageByMergeJob

Make the parentage link between a file and all the inputs of a given job
"""




from WMCore.WMBS.MySQL.Files.SetParentageByMergeJob import SetParentageByMergeJob as MySQLSetParentageByMergeJob

class SetParentageByMergeJob(MySQLSetParentageByMergeJob):
    pass
