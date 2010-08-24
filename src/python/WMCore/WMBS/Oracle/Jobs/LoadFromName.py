#!/usr/bin/env python
"""
_LoadFromName_

Oracle implementation of Jobs.LoadFromName.
"""

__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.2 2009/01/12 19:26:04 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Jobs.LoadFromName import LoadFromName as LoadFromNameJobMySQL

class LoadFromName(LoadFromNameJobMySQL):
    """
    _LoadFromName_

    Retrieve meta data for a job given it's name.  This includes the name,
    job group and last update time.  This will also retrieve the job mask
    if one exists.
    """
    sql = LoadFromNameJobMySQL.sql
