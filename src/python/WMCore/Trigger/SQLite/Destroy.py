#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for destroying SQLite specific schema for the trigger

"""

__revision__ = "$Id: Destroy.py,v 1.1 2009/05/14 15:46:13 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"


from WMCore.Trigger.MySQL import Destroy as MySQLDestroy

class Destroy(MySQLDestroy):
    """
    _Create_
    
    Class for destroying SQLite specific schema for the trigger
    """
    
    
    

