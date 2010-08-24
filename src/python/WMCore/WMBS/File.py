#!/usr/bin/env python
"""
_File_

A simple object representing a file in WMBS

"""

__revision__ = "$Id: File.py,v 1.2 2008/05/02 13:47:28 metson Exp $"
__version__ = "$Revision: 1.2 $"

class File(object):
    lfn = ''
    id = -1
    size = 0 
    events = 0
    run = 0
    lumi = 0
    
    def __init__(self, lfn='', id=-1, size=0, events=0, run=0, lumi=0):
        """
        Create the file object
        """
        self.id = id
        self.lfn = lfn
        self.size = size 
        self.events = events
        self.run = run
        self.lumi = lumi
    
    def getInfo(self):
        """
        Return the files attributes as a tuple
        """
        return self.lfn, self.id, self.size, self.events, self.run, self.lumi