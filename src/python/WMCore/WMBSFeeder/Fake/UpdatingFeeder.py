#!/usr/bin/env python
"""
_FakeFeeder_

A feeder implementation that generates (1000 is default) fake files. Make up random sizes, names and locations etc.

Returns existing files, with updated locations.
"""
__all__ = []
__revision__ = "$Id: UpdatingFeeder.py,v 1.1 2008/07/21 17:25:41 metson Exp $"
__version__ = "$Revision: 1.1 $"

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
    def __init__(self, max = 10, num_files=1000):
        self.max = max
        self.locations = ['fakese01.cern.ch','fakese02.cern.ch','fakese01.fnal.gov','fakese02.fnal.gov','fakese01.rl.ac.uk','fakese02.rl.ac.uk']
        self.files = []
        for i in range(0, num_files):
            lfn='/store/data/fake-feeder-files/notreal/%s.root' % uuid(i) 
            size=2000 + ((i-5) * 50) 
            events=1000 + ((i-3) * 150) 
            run = random.randint(0 , int(3.14159265 * i * self.max)) 
            lumi = random.randint(0 ,10)
            file = File(lfn, size, events, run, lumi)
            self.files.append(file)
        
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
                fileid = random.randint(0 , len(self.files)-1)
                file = self.files[fileid]
                file.setLocation(locs)
                list.append(file)
            f.addFile(list)
                
            