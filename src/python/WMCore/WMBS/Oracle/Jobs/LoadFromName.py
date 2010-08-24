#!/usr/bin/env python
"""
_LoadFromName_

Oracle implementation of Jobs.LoadFromName.
"""

__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.1 2008/11/24 21:51:40 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.LoadFromName import LoadFromName as LoadFromNameJobMySQL

class LoadFromName(LoadFromNameJobMySQL):
    """
    _LoadFromName_

    Retrieve meta data for a job given it's name.  This includes the name,
    job group and last update time.  This will also retrieve the job mask
    if one exists.
    """
    sql = LoadFromNameJobMySQL.sql