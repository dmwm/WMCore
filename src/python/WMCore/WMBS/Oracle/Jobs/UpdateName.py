
#!/usr/bin/env python
"""
_UpdateName_
Oracle implementation of Jobs.UpdateName
"""
__all__ = []
__revision__ = "$Id: UpdateName.py,v 1.2 2008/12/05 21:06:25 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Jobs.UpdateName import UpdateName as UpdateNameJobMySQL

class UpdateName(UpdateNameJobMySQL):
    sql = UpdateNameJobMySQL.sql
    