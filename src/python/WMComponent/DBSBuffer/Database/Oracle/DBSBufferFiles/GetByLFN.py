#!/usr/bin/env python
"""
_GetByLFN_

Oracle implementation of GetByLFN
"""

__revision__ = "$Id: GetByLFN.py,v 1.3 2009/07/14 19:12:29 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetByLFN import GetByLFN as MySQLGetByLFN

class GetByLFN(MySQLGetByLFN):
    """

    Oracle implementation of GetByLFN

    """
    pass
