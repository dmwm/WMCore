#!/usr/bin/env python
"""
_Request.FindByPrepID_

API for finding a new request by Prep ID
"""
from WMCore.RequestManager.RequestDB.MySQL.Request.FindByPrepID import FindByPrepID as FindByPrepIDMySQL

class FindByPrepID(FindByPrepIDMySQL):
    pass
