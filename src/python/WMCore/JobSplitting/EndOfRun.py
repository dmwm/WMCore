#!/usr/bin/env python
"""
_EndOfRun_

If a subscription's fileset is closed, make a job that will run over any available
files
"""

__revision__ = "$Id: EndOfRun.py,v 1.7 2010/05/07 14:37:27 mnorman Exp $"
__version__  = "$Revision: 1.7 $"

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
        unacquired ('available') files if the fileset is marked as closed
        returns nothing otherwise.

        Can take files_per_job as an argument
        """

        filesPerJob  = int(kwargs.get("files_per_job", 999999))
       
        #  //
        # // get the fileset
        #//
        fileset = self.subscription.getFileset()

        try:
            fileset.load()
        except AttributeError, ae:
            pass

        # Return if you have the fileset still open
        if fileset.open:
            logging.error("Fileset was open")
            return

        # Get a dictionary of sites, files
        locationDict = self.sortByLocation()

        # Get a new jobGroup
        self.newGroup()

        jobCount = 0
        baseName = makeUUID()

        # Unlike FileBased, all these jobs will be in one group
        for location in locationDict.keys():
            # For each location, though, we'll need new jobs.
            fileList    = locationDict[location]
            filesInJob  = 0
            if len(fileList) == 0:
                #No files for this location
                #This isn't supposed to happen, but better safe then sorry
                continue
            for f in fileList:
                if filesInJob == 0 or filesInJob >= filesPerJob:
                    self.newJob(name = '%s-endofrun-%i' %(baseName, jobCount))
                    filesInJob = 0
                    jobCount += 1
                f.loadData(parentage = 1)
                self.currentJob.addFile(f)
                filesInJob += 1

        return

