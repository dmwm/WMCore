#!/usr/bin/env python
"""
_Harvest_

"""
import threading
import os

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID
from WMCore.DAOFactory import DAOFactory


class Harvest(JobFactory):
    """
    _Harvest_

    Job splitting algoritm which creates a single job for all files
    in the fileset (not neccessarily just available files).
    Two distinct modes, Periodic and EndOfRun.

    In Periodic mode, we periodically create a job processing all
    files. A job will not be created until the previous job (if
    there is one) has been completed and there are new available
    files in the fileset. The specified period is the amount of
    time in seconds between the end of a job and the creation of
    another job.

    In EndOfRun mode, create a job processing all files once the
    input file has been closed. This means there will only be
    a single job in total for the subscription.

    For the EndOfRun mode support a sibling parameters that is
    set if there is also a Periodic subscription. In this case
    wait until the Periodic subscription is finished before
    triggering the EndOfRun harvesting.

    """

    def createJobsLocationWise(self, fileset, endOfRun, dqmHarvestUnit):

        myThread = threading.currentThread()
        fileset.loadData(parentage = 0)
        allFiles = fileset.getFiles()

        # sort by location and run
        locationDict = {}
        runDict = {}
        for fileInfo in allFiles:

            locSet = frozenset(fileInfo['locations'])
            runSet = fileInfo.getRuns()

            #Cache run information
            runDict[fileInfo['lfn']] = runSet
            fileInfo['runs'] = set()

            if len(locSet) == 0:
                msg = "File %s has no locations!" % fileInfo['lfn']
                myThread.logger.error(msg)
            if len(runSet) == 0:
                msg = "File %s has no run information!" % fileInfo['lfn']
                myThread.logger.error(msg)

            # Populate a dictionary with [location][run] so we can split jobs according to those different combinations
            if locSet not in locationDict.keys():
                locationDict[locSet] = {}
            for run in runSet:
                if run.run in locationDict[locSet].keys():
                    locationDict[locSet][run.run].append(fileInfo)
                else:
                    locationDict[locSet][run.run] = [fileInfo]

        # create separate jobs for different locations
        self.newGroup()
        self.jobCount = 0
        baseName = makeUUID()
        self.newGroup()

        if endOfRun:
            harvestType = "EndOfRun"
        else:
            harvestType = "Periodic"

        for location in locationDict.keys():
            
            if dqmHarvestUnit == "byRun":
                self.createJobByRun(locationDict, location, baseName, harvestType, runDict, endOfRun)
            else:
                #TODO: need to add when specific run is specified.
                self.createMultiRunJob(locationDict, location, baseName, harvestType, runDict, endOfRun)

        return
    
    def createJobByRun(self, locationDict, location, baseName, harvestType, runDict, endOfRun):
        """
        _createJobByRun_

        Creates one job per run for all files available at the same location.
        """

        for run in locationDict[location].keys():
            # Should create at least one job for every location/run, putting this here will do
            self.jobCount += 1
            self.newJob(name = "%s-%s-Harvest-%i" % (baseName, harvestType, self.jobCount))
            for f in locationDict[location][run]:
                for fileRun in runDict[f['lfn']]:
                    if fileRun.run == run:
                        self.currentJob['mask'].addRun(fileRun)
                        break
                self.currentJob.addFile(f)

            if endOfRun:
                self.currentJob.addBaggageParameter("runIsComplete", True)
            self.mergeLumiRange(self.currentJob['mask']['runAndLumis'])
        return
    
    def createMultiRunJob(self, locationDict, location, baseName, harvestType, runDict, endOfRun):
        """
        _createMultiRunJob_

        Creates a single harvesting job for all files and runs available
        at the same location.
        """
        
        self.jobCount += 1
        self.newJob(name = "%s-%s-Harvest-%i" % (baseName, harvestType, self.jobCount))
        for run in locationDict[location]:
            for f in locationDict[location][run]:
                for fileRun in runDict[f['lfn']]:
                    if fileRun.run == run:
                        self.currentJob['mask'].addRun(fileRun)
                        break
                if f not in self.currentJob['input_files']:
                    self.currentJob.addFile(f)

        if endOfRun:
            self.currentJob.addBaggageParameter("runIsComplete", True)
        self.mergeLumiRange(self.currentJob['mask']['runAndLumis'])
        return

    def mergeLumiRange(self, runLumis):
        """
        _mergeLumiRange_

        Merges the interesection of lumi ranges.
        """
        for run, lumis in runLumis.iteritems():
            lumis.sort(key=lambda sublist: sublist[0])
            fixedLumis = [lumis[0]]
            for lumi in lumis:
                if (fixedLumis[-1][1] +1) >= lumi[0]:
                    fixedLumis[-1][1] = lumi[1]
                else:
                    fixedLumis.append(lumi)
            self.currentJob['mask']['runAndLumis'][run] = fixedLumis

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        """
        myThread = threading.currentThread()

        periodicInterval = kwargs.get("periodic_harvest_interval", 0)
        periodicSibling = kwargs.get("periodic_harvest_sibling", False)
        dqmHarvestUnit = kwargs.get("dqmHarvestUnit", "byRun")
        
        daoFactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        releasePeriodicJobDAO = daoFactory(classname = "JobSplitting.ReleasePeriodicJob")
        periodicSiblingCompleteDAO = daoFactory(classname = "JobSplitting.PeriodicSiblingComplete")

        fileset = self.subscription.getFileset()
        fileset.load()

        if periodicInterval and periodicInterval > 0:

            # Trigger the Periodic Job if
            #  * it is the first job OR
            #  * the last job ended more than periodicInterval seconds ago
            triggerJob = releasePeriodicJobDAO.execute(subscription = self.subscription["id"], period = periodicInterval)

            if triggerJob:
                myThread.logger.debug("Creating Periodic harvesting job")
                self.createJobsLocationWise(fileset, False, dqmHarvestUnit)

        elif not fileset.open:

            # Trigger the EndOfRun job if
            #  * (same as Periodic to not have JobCreator go nuts and stop after the first iteration)  
            #  * there is no Periodic sibling subscription OR 
            #  * the Periodic sibling subscription is complete
            triggerJob = releasePeriodicJobDAO.execute(subscription = self.subscription["id"], period = 3600)
            if triggerJob and periodicSibling:
                triggerJob = periodicSiblingCompleteDAO.execute(subscription = self.subscription["id"])

            if triggerJob:
                myThread.logger.debug("Creating EndOfRun harvesting job")
                self.createJobsLocationWise(fileset, True, dqmHarvestUnit)

        return
