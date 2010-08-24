#!/usr/bin/env python
"""
_FakeFeeder_

A feeder implementation that generates fake files. Make up random sizes, names and locations etc.

Always returns new/unique files.
"""
__all__ = []
__revision__ = "$Id: Feeder.py,v 1.4 2008/08/13 15:22:05 metson Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.DataStructs.WMObject import WMObject
from WMCore.DataStructs.File import File
from WMCore.WMBSFeeder.FeederImpl import FeederImpl

import time, random, md5

def uuid( *args ):
    """
    Generates a universally unique ID.
    Any arguments only create more randomness.
    """
    t = long( time.time() * 1000 )
    r = long( random.random()*100000000000000000L )
    try:
      a = socket.gethostbyname( socket.gethostname() )
    except:
      # if we can't get a network address, just imagine one
      a = random.random()*100000000000000000L
    data = str(t)+' '+str(r)+' '+str(a)+' '+str(args)
    data = md5.md5(data).hexdigest()
    return data
  

class Feeder(FeederImpl):
    def __init__(self, max = 10):
        self.max = max
        self.locations = ['fakese01.cern.ch','fakese02.cern.ch','fakese01.fnal.gov','fakese02.fnal.gov','fakese01.rl.ac.uk','fakese02.rl.ac.uk']
    
    def __call__(self, fileset):
        """
        return a randomly sized list of files (DataStructs.File) at locations
        files will always be new
        """
        num_files = random.randint(0 , self.max)
        for f in self.makelist(fileset):
            list = []
            for i in range(0, num_files):
                # Decide where the file is
                locs = []
                for i in range(0, len(self.locations)):
                    if random.randint(0 , 1):
                        locs.append(self.locations[i])
                lfn='/store/data/fake-feeder-files/notreal/%s.root' % uuid(i) 
                size=2000 + ((i-5) * 50) 
                events=1000 + ((i-3) * 150) 
                run = random.randint(0 , int(3.14159265 * i * self.max)) 
                lumi = random.randint(0 ,10)
                file = File(lfn, size, events, run, lumi)
                file.setLocation(locs)
                f.addFile(file)
                
        return fileset
                
            