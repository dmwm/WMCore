#!/usr/bin/env python
"""
_GetParentIDsByID_

Oracle implementation of GetParentIDsByID
"""

__revision__ = "$Id: GetParentIDsByID.py,v 1.6 2009/12/16 17:45:41 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WMBS.MySQL.Files.GetParentIDsByID import GetParentIDsByID \
     as GetParentIDsMySQL

class GetParentIDsByID(GetParentIDsMySQL):
    pass
    
