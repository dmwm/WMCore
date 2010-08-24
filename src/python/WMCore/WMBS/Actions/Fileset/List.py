#!/usr/bin/env python
"""
_ListFilesetAction_

List filesets in WMBS
"""

__revision__ = "$Id: List.py,v 1.1 2008/06/09 16:13:43 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction

class ListFilesetAction(BaseAction):
    name = "Fileset.List"