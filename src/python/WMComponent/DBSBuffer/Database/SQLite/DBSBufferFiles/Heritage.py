#!/usr/bin/env python
"""

SQLite implementation of Heritage

"""

__revision__ = "$Id: Heritage.py,v 1.2 2009/07/13 19:31:54 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Heritage import Heritage as MySQLHeritage

class Heritage(MySQLHeritage):
    """

    SQLite implementation of Heritage

    """
    pass
