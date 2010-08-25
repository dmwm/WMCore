#!/usr/bin/env python
"""
_ListJobStates_

SQLite implementation of Monitoring.ListJobStates
"""




from WMCore.WMBS.MySQL.Monitoring.ListJobStates import ListJobStates \
    as ListJobStatesMySQL

class ListJobStates(ListJobStatesMySQL):
    pass
