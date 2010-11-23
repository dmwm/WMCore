#!/usr/bin/env python
"""
Oracle implementation of File.SetParentageByJob

Make the parentage link between a file and all the inputs of a given job
"""




from WMCore.WMBS.MySQL.Files.SetParentageByJob import SetParentageByJob as MySQLSetParentageByJob

class SetParentageByJob(MySQLSetParentageByJob):
    pass
