#!/usr/bin/env python
"""
_LoadForSubmitter_

Oracle function to load jobs for submission
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.LoadForSubmitter import LoadForSubmitter as MySQLLoadForSubmitter

class LoadForSubmitter(MySQLLoadForSubmitter):
    """
    _LoadForSubmitter_

    Oracle implementation of JobSubmitter specific loader
    """
