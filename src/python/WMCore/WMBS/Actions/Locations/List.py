#!/usr/bin/env python
"""
_ListLocationsAction_

Add a location to WMBS
"""

__revision__ = "$Id: List.py,v 1.1 2008/06/10 11:55:58 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction

class ListLocationsAction(BaseAction):
    name = "Locations.List"