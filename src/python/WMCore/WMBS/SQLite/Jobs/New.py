#!/usr/bin/env python
"""
_New_

SQLite implementation of Jobs.New
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.New import New as NewMySQL

class New(NewMySQL):
    pass
