#!/usr/bin/env python
"""
_ListSitesSlotsState_

Query the Resource Control database for Oracle to get all the current sites,
pending slots, running slots and state.
"""

from WMCore.ResourceControl.MySQL.ListSitesSlotsState import ListSitesSlotsState \
  as ListSitesSlotsStateMySQL
class ListSitesSlotsState(ListSitesSlotsStateMySQL):
    pass
