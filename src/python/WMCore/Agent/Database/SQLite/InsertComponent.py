"""
_InsertComponent_

SQLite implementation of InsertComponent
"""

__all__ = []



from WMCore.Agent.Database.MySQL.InsertComponent import InsertComponent \
     as InsertComponentMySQL

class InsertComponent(InsertComponentMySQL):
    pass
