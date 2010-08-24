
#!/usr/bin/env python
"""
_UpdateName_
MySQL implementation of Jobs.UpdateName
"""
__all__ = []
__revision__ = "$Id: UpdateName.py,v 1.1 2008/11/24 21:51:40 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.UpdateName import UpdateName as UpdateNameJobMySQL

class UpdateName(UpdateNameJobMySQL):
    sql = UpdateNameJobMySQL.sql
    