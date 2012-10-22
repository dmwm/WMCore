#!/usr/bin/env python
"""
Oracle implementation of Jobs.LoadOutputID

Load the ID of the output fileset for a job
"""
__all__ = []



from WMCore.WMBS.MySQL.Jobs.LoadOutputID import LoadOutputID as MySQLLoadOutputID

class LoadOutputID(MySQLLoadOutputID):
    """

    Load the ID of the output fileset for a job

    """
