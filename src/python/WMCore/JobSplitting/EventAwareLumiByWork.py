#!/usr/bin/env python
"""
_EventAwareLumiByWork_

Lumi based splitting algorithm that will chop a fileset into
a set of jobs based on lumi sections, failing jobs with too many
events in a lumi and adapting the number of lumis per job
according to the average number of events per lumi in the files.

This is adapted from EventAwareLumiBased but does its work on lumi sections directly, not files.
No correction for lumis split across files is needed, it's automatic. Makes use of the LumiList
class to simplify the code

"""

from __future__ import (division, print_function)

import operator

from future.utils import viewitems

import logging
from collections import defaultdict

from WMCore.DataStructs.LumiList import LumiList
from WMCore.DataStructs.Run import Run
from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File import File


class EventAwareLumiByWork(JobFactory):
    """
    Split jobs by lumis taking into account events per lumi
    """

    locations = []

    def __init__(self, package='WMCore.DataStructs', subscription=None, generators=None, limit=0):
        super(EventAwareLumiByWork, self).__init__(package, subscription, generators, limit)

        self.loadRunLumi = None  # Placeholder for DAO factory if needed
        self.collectionName = None  # Placeholder for ACDC Collection Name, if needed
        self.perfParameters = {}
        self.deterministicPU = False
        self.maxLumis = 1  # Maximum lumis seen in a job (needed for deterministic pileup only)
        self.maxEvents = 1  # Maximum events seen in a lumi (needed for deterministic pileup only)
        # TODO this might need to be configurable instead of being hardcoded
        self.defaultJobTimeLimit = 48 * 3600 # 48 hours

        # Job accumulators
        self.eventsInJob = 0
        self.jobLumis = []
        self.jobFiles = set()
        self.eventsInLumi = 0

        # Location accumulators. Using these two objects lets us automatically fix up the cases where a lumi
        # is split across two files
        self.lumisProcessed = set()  # Which run/lumi pairs already have jobs created for this location
        self.filesByLumi = {}  # Map by lumi of which files contain that lumi

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Split files into a number of lumis per job
        Allow a flag to determine if we split files between jobs
        """

        # TODO: This has one possible weakness. If two files (blocks) contain the same run/lumi section but are
        # TODO: split across sites two different jobs will read the same lumi

        avgEventsPerJob = int(kwargs.get('events_per_job', 5000))
        jobTimeLimit = int(kwargs.get('job_time_limit', self.defaultJobTimeLimit))
        totalEventLimit = int(kwargs.get('total_events', 0))
        splitOnFile = bool(kwargs.get('halt_job_on_file_boundaries', False))
        self.collectionName = kwargs.get('collectionName', None)
        splitOnRun = kwargs.get('splitOnRun', True)
        getParents = kwargs.get('include_parents', False)
        runWhitelist = kwargs.get('runWhitelist', [])
        runs = kwargs.get('runs', None)
        lumis = kwargs.get('lumis', None)
        self.deterministicPU = kwargs.get('deterministicPileup', False)
        self.perfParameters = kwargs.get('performance', {})

        # Calculate and add performance information
        self.timePerEvent, self.sizePerEvent, self.memoryRequirement = \
            self.getPerformanceParameters(self.perfParameters)

        if avgEventsPerJob <= 0:
            msg = "events_per_job parameter must be positive. Its value is: %d" % avgEventsPerJob
            raise RuntimeError(msg)

        # Set the lumi mask for the fileset based on ACDC or runs & lumis and/or runWhitelist
        lumiMask = LumiList()
        if self.collectionName:
            lumiMask = self.lumiListFromACDC(couchURL=kwargs.get('couchURL'), couchDB=kwargs.get('couchDB'),
                                             filesetName=kwargs.get('filesetName'), collectionName=self.collectionName)
        elif runs and lumis and runWhitelist:
            lumiMask = LumiList(wmagentFormat=(runs, lumis)) & LumiList(runs=runWhitelist)
        elif runs and lumis:
            lumiMask = LumiList(wmagentFormat=(runs, lumis))
        elif runWhitelist:
            lumiMask = LumiList(runs=runWhitelist)
        logging.debug('%s splitting with lumiMask%s%s', self.__class__.__name__, '\n' if bool(lumiMask) else ' ',
                      lumiMask)

        if self.package == 'WMCore.WMBS':
            self.loadRunLumi = self.daoFactory(classname="Files.GetBulkRunLumi")
            if self.deterministicPU:
                getJobNumber = self.daoFactory(classname="Jobs.GetNumberOfJobsPerWorkflow")
                self.nJobs = getJobNumber.execute(workflow=self.subscription.getWorkflow().id)
                logging.info('Creating jobs in DeterministicPileup mode for %s',
                             self.subscription.workflowName())

        filesByLocation = self.getFilesSortedByLocation(avgEventsPerJob)
        if not filesByLocation:
            logging.info("There are not enough events/files to be splitted. Trying again next cycle")
            return

        totalEvents = 0
        lastRun = None
        stopTask = False
        lastFile = None

        for location, filesAtLocation in viewitems(filesByLocation):
            self.newGroup()  # For each location, we need a new jobGroup
            self.eventsInJob = 0
            self.jobLumis = []
            self.jobFiles = set()
            self.lumisProcessed = set()
            if self.loadRunLumi:
                self.populateFilesFromWMBS(filesAtLocation)
            lumisByFile, eventsByLumi = self.fileLumiMaps(filesAtLocation=filesAtLocation, getParents=getParents,
                                                          lumiMask=lumiMask)
            # sort files by name, to have a more reproducible job creation
            for f in sorted(filesAtLocation, key=operator.itemgetter('lfn')):
                lfn = f['lfn']
                if lfn not in lumisByFile:
                    continue  # There are no lumis of interest in the file
                for run, lumi in lumisByFile[lfn].getLumis():
                    if (run, lumi) in self.lumisProcessed:
                        continue  # We already saw this lumi and it got included in an earlier job
                    self.eventsInLumi = eventsByLumi[run][lumi]

                    if 0 < totalEventLimit <= totalEvents:
                        stopTask = True
                        break  # Don't add this lumi to the job

                    totalEvents += self.eventsInLumi
                    self.maxEvents = max(self.maxEvents, self.eventsInLumi)
                    timePerLumi = self.eventsInLumi * self.timePerEvent
                    if timePerLumi > jobTimeLimit and len(lumisByFile[lfn].getLumis()):
                        # This lumi has too many events. Output this job and a new one with just that lumi
                        failReason = "File %s has a single lumi %s, in run %s " % (lfn, lumi, run)
                        failReason += "with too many events %d and it would take %d sec to run" \
                                      % (self.eventsInLumi, timePerLumi)
                        self.stopAndMakeJob(reason='Lumi too big', runLumi=(run, lumi),
                                            failNextJob=True, failReason=failReason)
                    elif abs(self.eventsInLumi + self.eventsInJob - avgEventsPerJob) >= abs(
                                    self.eventsInJob - avgEventsPerJob) \
                            and self.eventsInLumi > 0 and self.eventsInJob > 0:
                        # This lumi doesn't fit in this job (logic is to get as close as possible to avgEventsPerJob)
                        self.stopAndMakeJob(reason='Event limit', runLumi=(run, lumi))
                    elif splitOnRun and run != lastRun and self.eventsInJob > 0:
                        # This lumi is from a new run and we break on it
                        self.stopAndMakeJob(reason='Run change', runLumi=(run, lumi))
                    elif splitOnFile and self.filesByLumi[run][lumi][0]['lfn'] != lastFile and len(self.jobLumis):
                        # This lumi is from a new file and we break on it
                        self.stopAndMakeJob(reason='File change', runLumi=(run, lumi))
                    else:  # Keep going, add lumi to list of lumis for the job
                        for f2 in self.filesByLumi[run][lumi]:
                            self.jobFiles.add(f2)
                        self.jobLumis.append((run, lumi))
                        self.eventsInJob += self.eventsInLumi
                    lastRun = run
                    lastFile = lfn

            # Make the last job assuming its not just a failed job
            if self.eventsInJob > 0 or self.jobLumis or self.jobFiles:  # Make sure there is someting to do
                self.makeNewJobByWork(reason='End of list')
            if stopTask:
                break

        return

    def stopAndMakeJob(self, runLumi, failNextJob=False, reason=None, failReason=None):
        """
        Put out the previously accumulated info into a job and start a new job with the current lumi, events

        Args:
            runLumi: The current run and lumi
            failNextJob: The current lumi belongs in its own job too, so make two jobs
            reason: The reason for starting a new job
            failReason: The reason for making the second job

        Returns: nothing
        """

        (run, lumi) = runLumi
        # Make the job assuming its not just a failed job
        if self.eventsInJob > 0 or not failNextJob:
            self.makeNewJobByWork(reason=reason)

        # Now start on the next job
        self.eventsInJob = self.eventsInLumi  # Promote both to self
        self.jobLumis = [(run, lumi)]  # Promote both to self
        self.lumisProcessed.add((run, lumi))  # Promote to self
        self.jobFiles = set()  # Promote to self
        for f2 in self.filesByLumi[run][lumi]:  # promote filesByLumi to self
            self.jobFiles.add(f2)

        if failNextJob:  # Make a 2nd failed job
            self.makeNewJobByWork(reason=failReason, failedJob=True)
            self.eventsInJob = 0
            self.jobLumis = []
            self.jobFiles = set()

    def makeNewJobByWork(self, reason='', failedJob=False):
        """
        Make a new job given the passed in parameters.

        :param reason: Why are we making a new job (debugging only)
        :param failedJob: Make the job as already failed

        :return: nothing
        """

        events = self.eventsInJob
        lumis = self.jobLumis
        files = self.jobFiles

        self.maxLumis = max(self.maxLumis, len(lumis))

        # Transform the lumi list into something compact and usable
        lumiList = LumiList(lumis=lumis).getCompactList()
        logging.debug("Because %s new job with events: %s, lumis: %s, and files: %s",
                      reason, events, lumiList, [f['lfn'] for f in files])
        if failedJob:
            logging.debug(" This job will be made failed")
            self.newJob(failedJob=failedJob, failedReason=reason)
        else:
            self.newJob()

        self.currentJob.addResourceEstimates(jobTime=events * self.timePerEvent,
                                             disk=events * self.sizePerEvent,
                                             memory=self.memoryRequirement)
        # Add job mask information
        for run, lumiRanges in viewitems(lumiList):
            for lumiRange in lumiRanges:
                self.currentJob['mask'].addRunAndLumis(run=int(run), lumis=lumiRange)
        # Add files
        for f in files:
            self.currentJob.addFile(f)
        # Add pileup info if needed
        if self.deterministicPU:
            eventsToSkip = (self.nJobs - 1) * self.maxEvents * self.maxLumis
            logging.debug('Adding baggage to skip %s events', eventsToSkip)
            self.currentJob.addBaggageParameter("skipPileupEvents", eventsToSkip)

        return

    @staticmethod
    def countLumis(runs):
        """
        Count up the number of lumi sections in a runs object
        :param runs:
        :return count:
        """
        count = 0
        for runLumi in runs:
            count += len(runLumi.lumis)
        return count

    @staticmethod
    def lumiListFromACDC(couchURL=None, couchDB=None, filesetName=None, collectionName=None):
        """
        This is not implemented yet
        :return:
        """
        from WMCore.ACDC.DataCollectionService import DataCollectionService

        goodRunList = None
        try:
            logging.info('Creating jobs for ACDC fileset %s', filesetName)
            dcs = DataCollectionService(couchURL, couchDB)
            goodRunList = dcs.getLumilistWhitelist(collectionName, filesetName)
        except Exception as ex:
            msg = "Exception while trying to load goodRunList. "
            msg += "Refusing to create any jobs.\nDetails: %s" % ex.__str__()
            logging.exception(msg)

        return goodRunList

    def fileLumiMaps(self, filesAtLocation=None, getParents=False, lumiMask=None):
        """
        Args:
            filesAtLocation: the list of file objects at a particular location
            getParents: Attach a list of parents files to the file
            lumiMask: the lumiMask for the step. Files excluded by the lumiMask are dropped

        Returns:
            lumisByFile: LumiList for each LFN
            eventsByLumi: Estimate (for now) of how many events are in each lumi
        """
        lumisByFile = {}
        eventsByLumi = defaultdict(lambda: defaultdict(float))
        self.filesByLumi = defaultdict(lambda: defaultdict(list))

        for fileObj in filesAtLocation:
            lfn = fileObj['lfn']
            eventsInFile = fileObj['events']
            lumisInFile = self.countLumis(fileObj['runs'])
            if getParents:
                parentLFNs = self.findParent(lfn=lfn)
                for lfn in parentLFNs:
                    parent = File(lfn=lfn)
                    fileObj['parents'].add(parent)
            runsAndLumis = {str(runLumi.run): runLumi.lumis for runLumi in fileObj['runs']}
            lumiList = LumiList(runsAndLumis=runsAndLumis)
            if lumiMask:  # Apply the mask if there is one
                lumiList &= lumiMask
            if lumiList:  # Skip files with no lumis of interest
                lumisByFile[lfn] = lumiList

                for runLumi in fileObj['runs']:
                    run = runLumi.run
                    for lumi in runLumi.lumis:
                        if (runLumi.run, lumi) in lumiList:
                            self.filesByLumi[run][lumi].append(fileObj)
                            eventsByLumi[run][lumi] = int(round(eventsInFile / lumisInFile))

        return lumisByFile, eventsByLumi

    def populateFilesFromWMBS(self, filesByLocation):
        """
        Load the lumi information for files from WMBS

        Args:
            filesByLocation: the files at the location currently under consideration

        Returns: nothing
        """

        fileLumis = self.loadRunLumi.execute(files=filesByLocation)
        if not fileLumis:
            logging.warning("Empty fileLumis dict for workflow %s, subs %s.",
                            self.subscription.workflowName(), self.subscription['id'])
        for f in filesByLocation:
            lumiDict = fileLumis.get(f['id'], {})
            for run in lumiDict:
                f.addRun(run=Run(run, *lumiDict[run]))
