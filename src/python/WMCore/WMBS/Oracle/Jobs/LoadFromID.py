#!/usr/bin/env python
"""
_LoadFromID_

Oracle implementation of Jobs.LoadFromID.
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.2 2009/01/12 19:26:04 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Jobs.LoadFromID import LoadFromID as LoadFromIDJobMySQL

class LoadFromID(LoadFromIDJobMySQL):
    """
    _LoadFromID_

    Retrieve meta data for a job given it's ID.  This includes the name,
    job group and last update time.  This will also retrieve the job mask
    if one exists.
    """
    sql = LoadFromIDJobMySQL.sql
