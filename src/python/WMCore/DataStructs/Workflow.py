#!/usr/bin/env python
"""
_Workflow_

A class that describes some work to be undertaken on some files

"""
__all__ = []
__revision__ = "$Id: Workflow.py,v 1.2 2008/09/29 16:07:04 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.DataStructs.Pickleable import Pickleable
class Workflow(Pickleable):
    def __init__(self, spec=None, owner=None, name='workflow'):        
        #TODO: define a url-like scheme for spec's and enforce it here
        self.spec = spec
        self.name = name
        self.owner = owner