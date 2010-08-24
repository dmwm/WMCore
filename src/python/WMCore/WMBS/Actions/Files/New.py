#!/usr/bin/env python
"""
_NewFilesAction_

Add a (list of) new file(s) to WMBS
"""

__revision__ = "$Id: New.py,v 1.2 2008/06/12 10:05:44 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.Actions.Action import BoundAction
from WMCore.DAOFactory import DAOFactory

class NewFileAction(BoundAction):
    name = "Files.Add"