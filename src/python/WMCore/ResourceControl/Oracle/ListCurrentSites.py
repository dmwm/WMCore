#!/usr/bin/env python
"""
_ListCurrentSites_

This module list the current sites in Resource Control for Oracle
"""


from WMCore.ResourceControl.MySQL.ListCurrentSites import ListCurrentSites \
  as ListCurrentSitesMySQL
class ListCurrentSites(ListCurrentSitesMySQL):
    pass
