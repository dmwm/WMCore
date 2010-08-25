#!/usr/bin/env python
"""
_FixedDelay_

If the current time is beyond trigger_time, add
any unacquired jobs to 
"""

__revision__ = "$Id: FixedDelay.py,v 1.5 2009/12/16 18:55:44 sfoulkes Exp $"
__version__  = "$Revision: 1.5 $"

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID
import time

class FixedDelay(JobFactory):
    """
    if the current time is past trigger_time, 
    pull all the available files into a
    new job
    """
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        A time delay job splitting algorithm, will shove all unacquired
        files into a new job if the trigger_time has been passed
        """

        #  //
        # // get the fileset
        #//
        fileset = self.subscription.getFileset()
        trigger_time = kwargs['trigger_time']
        if (trigger_time < time.time()):
            availFiles = self.subscription.availableFiles()
            if (len(availFiles) == 0):
                # no files to acquire
                return []
                
            baseName = makeUUID()
            self.newGroup()
            self.newJob(name = '%s-endofrun' % (baseName,))
            
            for f in availFiles:                    
                self.currentJob.addFile(f)
