#!/usr/bin/env python
"""
_EventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts
"""

from builtins import str
from builtins import range
import logging
import traceback
from math import ceil

from WMCore.DataStructs.Run import Run
from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File import File


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
        getParents = kwargs.get("include_parents", False)
        lheInput = kwargs.get("lheInputFiles", False)
        collectionName = kwargs.get('collectionName', None)
        timePerEvent, sizePerEvent, memoryRequirement = \
            self.getPerformanceParameters(kwargs.get('performance', {}))
        acdcFileList = []
        deterministicPileup = kwargs.get('deterministicPileup', False)

        if eventsPerJob <= 0 or eventsPerLumi <= 0:
            msg = "events_per_job and events_per_lumi must be positive. Their values are: "
            msg += "events_per_job: %d, events_per_lumi: %d" % (eventsPerJob, eventsPerLumi)
            raise RuntimeError(msg)

        if deterministicPileup and self.package == 'WMCore.WMBS':
            getJobNumber = self.daoFactory(classname="Jobs.GetNumberOfJobsPerWorkflow")
            self.nJobs = getJobNumber.execute(workflow=self.subscription.getWorkflow().id)
            logging.info('Creating jobs in DeterministicPileup mode for %s',
                         self.subscription.workflowName())

        # If we have runLumi info, we need to load it from couch
        if collectionName:
            try:
                from WMCore.ACDC.DataCollectionService import DataCollectionService
                couchURL = kwargs.get('couchURL')
                couchDB = kwargs.get('couchDB')
                filesetName = kwargs.get('filesetName')
                collectionName = kwargs.get('collectionName')
                logging.info('Loading ACDC info for collectionName: %s, with filesetName: %s', collectionName,
                             filesetName)
                dcs = DataCollectionService(couchURL, couchDB)
                acdcFileList = dcs.getProductionACDCInfo(collectionName, filesetName)
            except Exception as ex:
                msg = "Exception while trying to load goodRunList\n"
                msg += "Refusing to create any jobs.\n"
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
                    # We have one MCFakeFile, then it needs run information
                    getRunLumiInformation = True
                    break
            if getRunLumiInformation:
                if self.package == 'WMCore.WMBS':
                    loadRunLumi = self.daoFactory(classname="Files.GetBulkRunLumi")
                    fileLumis = loadRunLumi.execute(files=fileList)
                    if not fileLumis:
                        logging.warning("Empty fileLumis dict for workflow %s, subs %s.",
                                        self.subscription.workflowName(), self.subscription['id'])
                    for f in fileList:
                        lumiDict = fileLumis.get(f['id'], {})
                        for run in lumiDict:
                            f.addRun(run=Run(run, *lumiDict[run]))

            for f in fileList:
                currentEvent = f['first_event']
                eventsInFile = f['events']
                runs = list(f['runs'])
                # We got the runs, clean the file.
                f['runs'] = set()

                if getParents:
                    parentLFNs = self.findParent(lfn=f['lfn'])
                    for lfn in parentLFNs:
                        parent = File(lfn=lfn)
                        f['parents'].add(parent)

                if acdcFileList:
                    totalJobs = self.createACDCJobs(f, acdcFileList, timePerEvent,
                                                    sizePerEvent, memoryRequirement,
                                                    lheInput, eventsPerJob, eventsPerLumi,
                                                    deterministicPileup, totalJobs)
                    continue
                if not f['lfn'].startswith("MCFakeFile"):
                    # there might be files with 0 event that still have to be processed
                    if eventsInFile == 0:
                        self.newJob(name=self.getJobName(length=totalJobs))
                        self.currentJob.addFile(f)
                        # Do not set LastEvent
                        self.currentJob["mask"].setMaxAndSkipEvents(None, currentEvent)
                        self.currentJob.addResourceEstimates(jobTime=0,
                                                             memory=memoryRequirement,
                                                             disk=0)
                        if deterministicPileup:
                            self.currentJob.addBaggageParameter("skipPileupEvents", (self.nJobs - 1) * eventsPerJob)
                        totalJobs += 1
                        logging.info("Job created for 0-event input file with %s", self.currentJob)
                    # Very very uncommon in production, but it has real input dataset
                    while eventsInFile:
                        self.newJob(name=self.getJobName(length=totalJobs))
                        self.currentJob.addFile(f)
                        if eventsInFile >= eventsPerJob:
                            jobTime = eventsPerJob * timePerEvent
                            diskRequired = eventsPerJob * sizePerEvent
                            self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob - 1, currentEvent)
                        else:
                            jobTime = eventsInFile * timePerEvent
                            diskRequired = eventsInFile * sizePerEvent
                            self.currentJob["mask"].setMaxAndSkipEvents(eventsInFile - 1, currentEvent)
                            eventsInFile = eventsPerJob
                        self.currentJob.addResourceEstimates(jobTime=jobTime,
                                                             memory=memoryRequirement,
                                                             disk=diskRequired)
                        if deterministicPileup:
                            self.currentJob.addBaggageParameter("skipPileupEvents", (self.nJobs - 1) * eventsPerJob)

                        eventsInFile -= eventsPerJob
                        currentEvent += eventsPerJob
                        totalJobs += 1
                        logging.debug("Job created for real input with %s", self.currentJob)
                else:
                    # This assumes there's only one run which is the case for MC
                    lumis = runs[0].lumis
                    (firstLumi, lastLumi) = (min(lumis), max(lumis))
                    currentLumi = firstLumi
                    lumisPerJob = int(ceil(float(eventsPerJob) / eventsPerLumi))

                    while eventsInFile:
                        self.newJob(name=self.getJobName(length=totalJobs))
                        self.currentJob.addFile(f)
                        self.currentJob.addBaggageParameter("lheInputFiles", lheInput)

                        # Limit the number of events to a unsigned 32bit int
                        if (currentEvent + eventsPerJob - 1) > (2 ** 32 - 1) and \
                                        (currentEvent + eventsInFile) > (2 ** 32 - 1):
                            currentEvent = 1

                        if eventsInFile >= eventsPerJob:
                            jobTime = eventsPerJob * timePerEvent
                            diskRequired = eventsPerJob * sizePerEvent
                            # Alan on 16/Apr/2019: inclusiveMask must be a real inclusiveMask, thus
                            # FirstEvent/FirstLumi and LastEvent/LastLumi are also processed by the job
                            self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob - 1, currentEvent)
                            self.currentJob["mask"].setMaxAndSkipLumis(lumisPerJob - 1, currentLumi)
                        else:
                            jobTime = eventsInFile * timePerEvent
                            diskRequired = eventsInFile * sizePerEvent
                            lumisPerJob = int(ceil(float(eventsInFile) / eventsPerLumi))
                            self.currentJob["mask"].setMaxAndSkipEvents(eventsInFile - 1, currentEvent)
                            self.currentJob["mask"].setMaxAndSkipLumis(lumisPerJob - 1, currentLumi)
                            eventsInFile = eventsPerJob

                        self.currentJob.addResourceEstimates(jobTime=jobTime,
                                                             memory=memoryRequirement,
                                                             disk=diskRequired)
                        if deterministicPileup:
                            self.currentJob.addBaggageParameter("skipPileupEvents", (self.nJobs - 1) * eventsPerJob)

                        eventsInFile -= eventsPerJob
                        currentEvent += eventsPerJob
                        currentLumi += lumisPerJob
                        totalJobs += 1
                        logging.info("Job created with mask: %s", self.currentJob['mask'])

        return

    def createACDCJobs(self, fakeFile, acdcFileInfo, timePerEvent, sizePerEvent,
                       memoryRequirement, lheInputOption, eventsPerJob,
                       eventsPerLumi, deterministicPileup, totalJobs=0):
        """
        _createACDCJobs_

        Create ACDC production jobs, these are treated differently
        since it is an exact copy of the failed jobs.
        """
        lumisPerJob = 0  # calculated a bit beyond
        for acdcFile in acdcFileInfo:
            if fakeFile['lfn'] == acdcFile['lfn']:
                eventsToRun = acdcFile["events"]
                currentEvent = acdcFile["first_event"]
                acdcFile["lumis"] = sorted(acdcFile["lumis"])
                while eventsToRun > 0:
                    ### WARNING: it assumes there will NOT be splitting changes between
                    # the original and the ACDC workflow
                    # Lumis to recover are not necessarily sequential
                    acdcFile["lumis"] = acdcFile["lumis"][lumisPerJob:]
                    currentLumi = acdcFile["lumis"][0]
                    self.newJob(name=self.getJobName(length=totalJobs))
                    self.currentJob.addFile(fakeFile)
                    self.currentJob.addBaggageParameter("lheInputFiles", lheInputOption)
                    # Limit the number of events to a unsigned 32bit int
                    if (currentEvent + eventsPerJob) > (2 ** 32 - 1):
                        currentEvent = 1
                    if eventsToRun >= eventsPerJob:
                        self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob - 1, currentEvent)
                    else:
                        self.currentJob["mask"].setMaxAndSkipEvents(eventsToRun - 1, currentEvent)
                    if fakeFile['lfn'].startswith("MCFakeFile"):
                        # either a shorter last job or a normal sized one
                        lumisPerJob = int(ceil(float(min(eventsToRun, eventsPerJob)) / eventsPerLumi))
                        # I don't like it! but if we want to reduce the memory footprint, we need
                        # to keep merging MCFakeFiles at DataCollection level. Which requires a dirty
                        # lumi check - for sequential lumis - in here
                        while True:
                            setLumis = set(range(currentLumi, currentLumi + lumisPerJob))
                            if setLumis.issubset(set(acdcFile["lumis"])):
                                break
                            else:
                                lumisPerJob -= 1
                        self.currentJob["mask"].setMaxAndSkipLumis(lumisPerJob - 1, currentLumi)
                    jobTime = eventsPerJob * timePerEvent
                    diskRequired = eventsPerJob * sizePerEvent
                    self.currentJob.addResourceEstimates(jobTime=jobTime,
                                                         memory=memoryRequirement,
                                                         disk=diskRequired)
                    if deterministicPileup:
                        self.currentJob.addBaggageParameter("skipPileupEvents", (self.nJobs - 1) * eventsPerJob)
                    logging.info("ACDC job created with mask: %s", self.currentJob['mask'])
                    eventsToRun -= eventsPerJob
                    currentEvent += eventsPerJob
                    currentLumi += lumisPerJob
                    totalJobs += 1
        return totalJobs
