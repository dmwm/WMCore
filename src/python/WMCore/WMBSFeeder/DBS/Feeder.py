#!/usr/bin/env python
"""
_FakeFeeder_

A feeder implementation that generates fake files. Make up random sizes, names and locations etc.

Always returns new/unique files.
"""
__all__ = []
__revision__ = "$Id: Feeder.py,v 1.1 2008/07/21 16:37:37 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.DataStructs.File import File
from WMCore.WMBSFeeder.FeederImpl import FeederImpl

class Feeder(FeederImpl):
    def __init__(self, dbsurl):
        self.dbs = dbsurl
    
    def __call__(self, fileset):
        """
        contact self.dbs for updated information on all files in fileset
        """
