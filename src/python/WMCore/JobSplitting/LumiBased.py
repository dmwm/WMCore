#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
_LumiBased_

Lumi based splitting algorithm that will chop a fileset into
a set of jobs based on lumi sections
"""

__revision__ = "$Id: LumiBased.py,v 1.21 2010/07/13 14:32:45 sfoulkes Exp $"
__version__  = "$Revision: 1.21 $"

import operator

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.DataStructs.Fileset import Fileset
from WMCore.Services.UUID import makeUUID

class LumiBased(JobFactory):
    """
    Split jobs by number of events
    """

    locations = []


    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Split files into a number of lumis per job
        Allow a flag to determine if we split files between jobs
        """


        lumisPerJob  = int(kwargs.get('lumis_per_job', 1))
        splitFiles   = bool(kwargs.get('split_files_between_job', False))

        lDict = self.sortByLocation()
        locationDict = {}

        for key in lDict.keys():
            newlist = []
            for f in lDict[key]:
                if hasattr(f, 'loadData'):
                    f.loadData()
                if len(f['runs']) == 0:
                    continue
                f['runs'] = sorted(f['runs'])
                for run in f['runs']:
                    run.lumis.sort()
                f['lowestRun'] = f['runs'][0]
                #f['lowestRun'] = list(sorted(f['runs']))[0]
                newlist.append(f)
            locationDict[key] = sorted(newlist, key=operator.itemgetter('lowestRun'))




        if splitFiles:
            self.withFileSplitting(lumisPerJob = lumisPerJob,
                                   locationDict = locationDict)
        else:
            self.noFileSplitting(lumisPerJob = lumisPerJob,
                                 locationDict = locationDict)

        return
        

                


    def noFileSplitting(self, lumisPerJob, locationDict):
        """
        Split files into jobs by lumi without splitting files

        Will create jobs with AT LEAST that number of lumis

        if lumisPerJob = 3:
        2 files of 3 lumis each  = 2 jobs
        2 files of 2 lumis each  = 1 job
        2 files of 1 lumi each   = 1 job
        10 files of 1 lumi each  = 4 jobs
        """

        totalJobs    = 0
        for location in locationDict.keys():

            # Create a new jobGroup
            self.newGroup()

            # Start this out high so we immediately create a new job
            lumisInJob  = lumisPerJob + 100

            # Hold the last lumi run
            lastRun    = None

            for f in locationDict[location]:
                fileLength = sum([ len(run) for run in f['runs']])
                if fileLength == 0:
                    # Then we have no lumis
                    # BORING.  Go home
                    continue

                fileRuns = list(f['runs'])
                if lumisInJob >= lumisPerJob:
                    # Then we need to close out this job
                    # And start a new job
                    if lastRun:
                        self.currentJob["mask"]['LastRun']   = lastRun.run
                        self.currentJob["mask"]['LastLumi']  = lastRun.lumis[-1]
                    self.newJob(name = self.getJobName(length=totalJobs))
                    firstRun = fileRuns[0]
                    self.currentJob['mask']['FirstRun']  = firstRun.run
                    self.currentJob['mask']['FirstLumi'] = firstRun.lumis[0]
                    lumisInJob = 0
                    totalJobs += 1


                # Actually add the file to the job
                self.currentJob.addFile(f)
                lumisInJob += fileLength
                lastRun = fileRuns[-1]

            if self.currentJob:
                # If we get to the end of the job, attach the last runs and lumis
                if lastRun:
                    self.currentJob["mask"]['LastRun']   = lastRun.run
                    self.currentJob["mask"]['LastLumi']  = lastRun.lumis[-1]

        return


    def withFileSplitting(self, lumisPerJob, locationDict):
        """
        Split files into jobs allowing one file to be in multiple jobs

        Creates jobs with EXACTLY lumisPerJob lumis
        """


        totalJobs = 0
        lastLumi = None
        lastRun = None
        for location in locationDict.keys():

            # Create a new jobGroup
            self.newGroup()

            # Start this out so we immediately create a new job
            lumisInJob  = lumisPerJob

            for f in locationDict[location]:

                if self.currentJob and not lumisInJob == lumisPerJob:
                        # Because merging can't handle multiple files in a job
                        # We have to start a new job when we get a new lumi
                        lumisInJob = lumisPerJob  # Do this so it auto-closes

                for run in f['runs']:
                    
                    for lumi in run:
                        # Now we're running through lumis
                        
                        if lumisInJob == lumisPerJob:
                            # Then we need to close out this job
                            # And start a new job
                            if lastRun != None and lastLumi != None:
                                self.currentJob["mask"]['LastRun']   = lastRun
                                self.currentJob["mask"]['LastLumi']  = lastLumi
                            self.newJob(name = self.getJobName(length=totalJobs))
                            self.currentJob['mask']['FirstRun']  = run.run
                            self.currentJob['mask']['FirstLumi'] = lumi
                            lumisInJob = 0
                            totalJobs += 1

                            # Add the file to new jobs
                            self.currentJob.addFile(f)

                        lumisInJob += 1

                        lastLumi = lumi
                        lastRun = run.run

            if self.currentJob:
                if lastRun and lastLumi:
                    self.currentJob["mask"]['LastRun']   = lastRun
                    self.currentJob["mask"]['LastLumi']  = lastLumi
                                
                        


        return




    

