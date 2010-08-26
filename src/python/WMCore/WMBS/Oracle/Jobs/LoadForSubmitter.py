#!/usr/bin/env python
"""
_LoadForSubmitter_

Oracle function to load jobs for submission
"""

__all__ = []
__revision__ = "$Id: LoadForSubmitter.py,v 1.1 2010/07/08 20:50:43 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.LoadForSubmitter import LoadForSubmitter as MySQLLoadForSubmitter

class LoadForSubmitter(MySQLLoadForSubmitter):
    """
    _LoadForSubmitter_

    Oracle implementation of JobSubmitter specific loader
    """
