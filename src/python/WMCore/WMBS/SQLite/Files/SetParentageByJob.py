#!/usr/bin/env python
"""
SQLite implementation of File.SetParentageByJob

Make the parentage link between a file and all the inputs of a given job
"""
__all__ = []



from WMCore.WMBS.MySQL.Files.SetParentageByJob import SetParentageByJob as MySQLSetParentageByJob

class SetParentageByJob(MySQLSetParentageByJob):
    """
    
    Make the parentage link between a file and all the inputs of a given job

    """
