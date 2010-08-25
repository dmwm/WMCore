#!/usr/bin/env python
"""
_EndOfRun_

If a subscription's fileset is closed, make a job that will run over any available
files
"""

__revision__ = "$Id: EndOfRun.py,v 1.3 2009/09/30 12:30:54 metson Exp $"
__version__  = "$Revision: 1.3 $"

import logging
from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID

class EndOfRun(JobFactory):
    """
    if a subscription's fileset is closed, pull all the available files into a
    new job
    """
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        An end-of-run job splitting algorithm, will return a job with all
        unacquired ("available") files if the fileset is marked as closed
        returns nothing otherwise
        """
       
        #  //
        # // Resulting job set (shouldnt this be a JobGroup??)
        #//
        self.newGroup()

        #  //
        # // get the fileset
        #//
        fileset = self.subscription.getFileset()
        try:
            fileset.load()
        except AttributeError, ae:
            logging.warning("Not using a WMBS fileset, hopefully this is a unit test")
            pass    

        if (not fileset.open):
            availFiles = self.subscription.availableFiles()

            baseName = makeUUID()
            self.newJob(name = '%s-endofrun' % (baseName,))
            
            if (len(availFiles) == 0):
                # no files to acquire
                return []
            
            for f in availFiles:                    
                self.currentJob.addFile(f)

            self.subscription.acquireFiles(availFiles)