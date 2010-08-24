#!/usr/bin/env python
"""
_LoadForSubmitter_

SQLite function to load jobs for submission
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.LoadForSubmitter import LoadForSubmitter as MySQLLoadForSubmitter

class LoadForSubmitter(MySQLLoadForSubmitter):
    """
    _LoadForSubmitter_

    SQLite implementation of JobSubmitter specific loader
    """
