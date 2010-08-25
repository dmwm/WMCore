#!/usr/bin/python
"""
_Create_

Class for creating SQLite specific schema for resource control.
"""

__revision__ = "$Id: Create.py,v 1.2 2010/02/09 17:59:15 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.ResourceControl.MySQL.Create import Create as MySQLCreate

class Create(MySQLCreate):
    pass
