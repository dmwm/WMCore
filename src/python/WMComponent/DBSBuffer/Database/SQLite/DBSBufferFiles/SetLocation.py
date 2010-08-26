#!/usr/bin/env python
"""

SQLite implementation of SetLocation

"""

__revision__ = "$Id: SetLocation.py,v 1.3 2009/07/13 19:31:54 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.SetLocation import SetLocation as MySQLSetLocation

class SetLocation(MySQLSetLocation):
    """

    SQLite implementation of SetLocation

    """
    pass
