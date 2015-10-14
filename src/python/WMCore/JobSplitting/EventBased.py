#!/usr/bin/env python
"""
_EventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts
"""

import logging
import traceback
from math import ceil

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File import File
from WMCore.DataStructs.Run import Run

class EventBased(JobFactory):
    """
    Split jobs by number of events
    """
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        An event base splitting algorithm.  All available files are split into a
        set number of events per job.
        """
        eventsPerJob = int(kwargs.get("events_per_job", 100))
        eventsPerLumi = int(kwargs.get("events_per_lumi", eventsPerJob))
        getParents   = kwargs.get("include_parents", False)
        lheInput = kwargs.get("lheInputFiles", False)
        collectionName  = kwargs.get('collectionName', None)
        timePerEvent, sizePerEvent, memoryRequirement = \
                    self.getPerformanceParameters(kwargs.get('performance', {}))
        acdcFileList = []
        deterministicPileup = kwargs.get('deterministicPileup', False)

        if deterministicPileup and self.package == 'WMCore.WMBS':
            getJobNumber = self.daoFactory(classname = "Jobs.GetNumberOfJobsPerWorkflow")
            self.nJobs = getJobNumber.execute(workflow = self.subscription.getWorkflow().id)
            logging.info('Creating %d jobs in DeterministicPileup mode' % self.nJobs)

        # If we have runLumi info, we need to load it from couch
        if collectionName:
            try:
                from WMCore.ACDC.DataCollectionService import DataCollectionService
                couchURL       = kwargs.get('couchURL')
                couchDB        = kwargs.get('couchDB')
                filesetName    = kwargs.get('filesetName')
                collectionName = kwargs.get('collectionName')
                owner          = kwargs.get('owner')
                group          = kwargs.get('group')
                logging.info('Creating jobs for ACDC fileset %s' % filesetName)
                dcs = DataCollectionService(couchURL, couchDB)
                acdcFileList = dcs.getProductionACDCInfo(collectionName, filesetName, owner, group)
            except Exception as ex:
                msg =  "Exception while trying to load goodRunList\n"
                msg +=  "Refusing to create any jobs.\n"
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                return

        totalJobs = 0

        locationDict = self.sortByLocation()
        for location in locationDict:
            self.newGroup()
            fileList = locationDict[location]
            getRunLumiInformation = False
            for f in fileList:
                if f['lfn'].startswith("MCFakeFile"):
                    #We have one MCFakeFile, then it needs run information
                    getRunLumiInformation = True
                    break
            if getRunLumiInformation:
                if self.package == 'WMCore.WMBS':
                    loadRunLumi = self.daoFactory(classname = "Files.GetBulkRunLumi")
                    fileLumis = loadRunLumi.execute(files = fileList)
                    for f in fileList:
                        lumiDict = fileLumis.get(f['id'], {})
                        for run in lumiDict.keys():
                            f.addRun(run = Run(run, *lumiDict[run]))

            for f in fileList:
                currentEvent = f['first_event']
                eventsInFile = f['events']
                runs = list(f['runs'])
                #We got the runs, clean the file.
                f['runs'] = set()

                if getParents:
                    parentLFNs = self.findParent(lfn = f['lfn'])
                    for lfn in parentLFNs:
                        parent = File(lfn = lfn)
                        f['parents'].add(parent)

                if acdcFileList:
                    if f['lfn'] in [x['lfn'] for x in acdcFileList]:
                        totalJobs = self.createACDCJobs(f, acdcFileList, timePerEvent,
                                                        sizePerEvent, memoryRequirement,
                                                        lheInput, eventsPerJob, eventsPerLumi, 
                                                        deterministicPileup, totalJobs)
                    continue
                if not f['lfn'].startswith("MCFakeFile"):
                    # Very very uncommon, but it has real input dataset
                    if eventsInFile >= eventsPerJob:
                        while currentEvent < eventsInFile:
                            self.newJob(name = self.getJobName(length=totalJobs))
                            self.currentJob.addFile(f)
                            if eventsPerJob + currentEvent < eventsInFile:
                                jobTime = eventsPerJob * timePerEvent
                                diskRequired = eventsPerJob * sizePerEvent
                                self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, currentEvent)
                            else:
                                jobTime = (eventsInFile - currentEvent) * timePerEvent
                                diskRequired = (eventsInFile - currentEvent) * sizePerEvent
                                self.currentJob["mask"].setMaxAndSkipEvents(None,
                                                                            currentEvent)
                            self.currentJob.addResourceEstimates(jobTime = jobTime,
                                                                 memory = memoryRequirement,
                                                                 disk = diskRequired)
                            if deterministicPileup:
                                self.currentJob.addBaggageParameter("skipPileupEvents", (self.nJobs - 1) * eventsPerJob)

                            logging.debug("Job created for real input with %s" % self.currentJob)
                            currentEvent += eventsPerJob
                            totalJobs    += 1
                    else:
                        self.newJob(name = self.getJobName(length=totalJobs))
                        self.currentJob.addFile(f)
                        jobTime = eventsInFile * timePerEvent
                        diskRequired = eventsInFile * sizePerEvent
                        self.currentJob.addResourceEstimates(jobTime = jobTime,
                                                             memory = memoryRequirement,
                                                             disk = diskRequired)
                        if deterministicPileup:
                            self.currentJob.addBaggageParameter("skipPileupEvents", (self.nJobs - 1) * eventsPerJob)
                        logging.debug("Last job created for real input with %s" % self.currentJob)
                        totalJobs += 1
                else:
                    #This assumes there's only one run which is the case for MC
                    lumis = runs[0].lumis
                    (firstLumi, lastLumi) = (min(lumis), max(lumis))
                    currentLumi = firstLumi
                    totalEvents = 0
                    if eventsInFile >= eventsPerJob:
                        while totalEvents < eventsInFile:
                            self.newJob(name = self.getJobName(length=totalJobs))
                            self.currentJob.addFile(f)
                            self.currentJob.addBaggageParameter("lheInputFiles",lheInput)
                            lumisPerJob = int(ceil(float(eventsPerJob)
                                                / eventsPerLumi))
                            #Limit the number of events to a unsigned 32bit int
                            eventsRemaining = eventsInFile - totalEvents
                            if (currentEvent + eventsPerJob - 1) > (2**32 - 1) and (currentEvent + eventsRemaining - 1) > (2**32 - 1):
                                currentEvent = 1
                            if eventsRemaining > eventsPerJob:
                                self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob,
                                                                            currentEvent)
                                self.currentJob["mask"].setMaxAndSkipLumis(lumisPerJob,
                                                                           currentLumi)
                                jobTime = eventsPerJob * timePerEvent
                                diskRequired = eventsPerJob * sizePerEvent
                            else:
                                jobTime = eventsRemaining * timePerEvent
                                diskRequired = eventsRemaining * sizePerEvent
                                lumisPerJob = int(ceil(float(eventsRemaining)/eventsPerLumi))
                                self.currentJob["mask"].setMaxAndSkipEvents(eventsRemaining,
                                                                            currentEvent)
                                self.currentJob["mask"].setMaxAndSkipLumis(lumisPerJob,
                                                                           currentLumi)

                            if deterministicPileup:
                                self.currentJob.addBaggageParameter("skipPileupEvents", (self.nJobs - 1) * eventsPerJob)
                            currentLumi  += lumisPerJob
                            currentEvent += eventsPerJob
                            totalEvents  += eventsPerJob
                            totalJobs    += 1
                            self.currentJob.addResourceEstimates(jobTime = jobTime,
                                                                 memory = memoryRequirement,
                                                                 disk = diskRequired)
                    else:
                        self.newJob(name = self.getJobName(length=totalJobs))
                        self.currentJob.addFile(f)
                        #For MC we use firstEvent instead of skipEvents so set it to 1
                        #We must check for events going over 2**32 - 1 here too
                        if (eventsInFile + currentEvent - 1) > (2**32 - 1):
                            currentEvent = 1
                        self.currentJob["mask"].setMaxAndSkipEvents(eventsInFile,
                                                                    currentEvent)
                        self.currentJob["mask"].setMaxAndSkipLumis(lastLumi -
                                                        currentLumi + 1, currentLumi)
                        jobTime = eventsInFile * timePerEvent
                        diskRequired = eventsInFile * sizePerEvent
                        self.currentJob.addResourceEstimates(jobTime = jobTime,
                                                             memory = memoryRequirement,
                                                             disk = diskRequired)
                        if deterministicPileup:
                            self.currentJob.addBaggageParameter("skipPileupEvents", (self.nJobs - 1) * eventsPerJob)
                        totalJobs += 1

    def createACDCJobs(self, fakeFile, acdcFileInfo, timePerEvent, sizePerEvent,
                       memoryRequirement, lheInputOption, eventsPerJob,
                       eventsPerLumi, deterministicPileup, totalJobs = 0):
        """
        _createACDCJobs_

        Create ACDC production jobs, this are treated differentely
        since it is an exact copy of the failed jobs.
        """
        for acdcFile in acdcFileInfo:
            if fakeFile['lfn'] == acdcFile['lfn']:
                eventsToRun = acdcFile["events"]
                currentEvent = acdcFile["first_event"]
                currentLumi = acdcFile["lumis"][0]
                lumisPerJob = 0
                while eventsToRun:
                    self.newJob(name = self.getJobName(length = totalJobs))
                    self.currentJob.addFile(fakeFile)
                    self.currentJob.addBaggageParameter("lheInputFiles", lheInputOption)
                    #Limit the number of events to a unsigned 32bit int
                    if (currentEvent + eventsPerJob) > (2**32 - 1):
                        currentEvent = 1
                    if eventsToRun >= eventsPerJob:
                        self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, currentEvent)
                        if fakeFile['lfn'].startswith("MCFakeFile"):
                            lumisPerJob = int(ceil(float(eventsPerJob)/eventsPerLumi))
                            self.currentJob["mask"].setMaxAndSkipLumis(lumisPerJob, currentLumi)
                    else:
                        self.currentJob["mask"].setMaxAndSkipEvents(eventsToRun, currentEvent)
                        if fakeFile['lfn'].startswith("MCFakeFile"):
                            lumisPerJob = int(ceil(float(eventsToRun)/eventsPerLumi))
                            self.currentJob["mask"].setMaxAndSkipLumis(lumisPerJob, currentLumi)
                        eventsToRun = eventsPerJob
                    jobTime = eventsPerJob * timePerEvent
                    diskRequired = eventsPerJob * sizePerEvent
                    self.currentJob.addResourceEstimates(jobTime = jobTime,
                                                         memory = memoryRequirement,
                                                         disk = diskRequired)
                    if deterministicPileup:
                        self.currentJob.addBaggageParameter("skipPileupEvents", (self.nJobs - 1) * eventsPerJob)
                    logging.info("ACDC job created with %s" % self.currentJob)
                    eventsToRun  -= eventsPerJob
                    currentEvent += eventsPerJob
                    currentLumi  += lumisPerJob
                    totalJobs    += 1
        return totalJobs
