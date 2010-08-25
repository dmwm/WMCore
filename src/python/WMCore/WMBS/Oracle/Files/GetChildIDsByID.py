#!/usr/bin/env python
"""
_GetChildIDsByID_

Oracle implementation of ChildIDsByID
"""

__revision__ = "$Id: GetChildIDsByID.py,v 1.4 2009/12/16 17:45:41 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Files.GetChildIDsByID import GetChildIDsByID \
     as GetChildIDsMySQL

class GetChildIDsByID(GetChildIDsMySQL):
    pass
