#!/usr/bin/env python
"""

Oracle implementation of GetByID
"""

__revision__ = "$Id: GetByID.py,v 1.2 2009/07/14 19:13:16 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetByID import GetByID as MySQLGetByID

class GetByID(MySQLGetByID):
    """

    Oracle implementation of GetByID
    """
    pass
