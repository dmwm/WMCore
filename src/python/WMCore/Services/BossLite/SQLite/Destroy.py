#!/bin/env python


from WMCore.JobStateMachine.MySQL.Destroy import Destroy as MySQLDestroy




class Destroy(MySQLDestroy):
    """
    This class is the SQLite implementation of the destruction operator for BossLite tables
    So far, it does nothing but call the MySQL implementation

    """
