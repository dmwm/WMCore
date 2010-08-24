#!/usr/bin/env python
"""
_File_

A simple object representing a file in WMBS

"""

__revision__ = "$Id: File.py,v 1.1 2008/05/01 17:31:24 metson Exp $"
__version__ = "$Revision: 1.1 $"

class File(object):
    size = 0 
    events = 0
    run = 0
    lumi = 0
    
    def __init__(self, lfn, size, events, run, lumi):
        """
        Create the file object
        """
        
        self.lfn = lfn
        self.size = size 
        self.events = events
        self.run = run
        self.lumi = lumi
        
    def getInfo(self):
        """
        Return the files attributes as a tuple
        """
        return self.lfn, self.size, self.events, self.run, self.lumi