#!/usr/bin/env python
"""
_LoadFromID_

Oracle implementation of Jobs.LoadFromID.
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.1 2008/11/24 21:51:41 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.LoadFromID import LoadFromID as LoadFromIDJobMySQL

class LoadFromID(LoadFromIDJobMySQL):
    """
    _LoadFromID_

    Retrieve meta data for a job given it's ID.  This includes the name,
    job group and last update time.  This will also retrieve the job mask
    if one exists.
    """
    sql = LoadFromIDJobMySQL.sql