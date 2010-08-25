#!/usr/bin/env python
"""
_LoadFileLocations_

SQLite implementation of Jobs.LoadFileLocations
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.LoadFileLocations import LoadFileLocations as MySQLLoadFileLocations

class LoadFileLocations(MySQLLoadFileLocations):
    """
    _LoadFileLocations_

    Retrieve all locations for a given job
    NOTE: THIS ASSUMES THAT ALL FILES HAVE IDENTICAL LOCATIONS!
    """


