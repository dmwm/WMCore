#!/usr/bin/env python
"""
_NewFilesetAction_

Add a fileset to WMBS
"""

__revision__ = "$Id: New.py,v 1.2 2008/06/12 10:04:47 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.Actions.Action import BoundAction
from WMCore.DAOFactory import DAOFactory

class NewFilesetAction(BoundAction):
    name = "Fileset.New"
        