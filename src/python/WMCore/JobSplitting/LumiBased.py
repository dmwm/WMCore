#!/usr/bin/env python
"""
_LumiBased_

Lumi based splitting algorithm that will chop a fileset into
a set of jobs based on lumi sections
"""

from __future__ import division
from builtins import range, object, int
from future.utils import viewitems, viewvalues

import logging
import operator

from Utils.IteratorTools import flattenList
from WMCore.DataStructs.Run import Run
from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File import File
from WMCore.WMSpec.WMTask import buildLumiMask


def isGoodLumi(goodRunList, run, lumi):
    """
    _isGoodLumi_

    Checks to see if runs match a run-lumi combination in the goodRunList
    This is a pain in the ass.
    """
    if goodRunList is None or goodRunList == {}:
        return True

    if not isGoodRun(goodRunList=goodRunList, run=run):
        return False

    for runRange in goodRunList.get(str(run), [[]]):
        # For each run range, which should have 2 elements
        if not len(runRange) == 2:
            # Then we're very confused and should exit
            logging.error("Invalid run range!  Failing this lumi!")
        elif runRange[0] <= lumi and runRange[1] >= lumi:
            # If a lumi is within a particular runRange, return true.
            return True
    return False


def isGoodRun(goodRunList, run):
    """
    _isGoodRun_

    Tell if this is a good run
    """
    if goodRunList is None or goodRunList == {}:
        return True

    if str(run) in goodRunList:
        # @e can find a run
        return True

    return False


class LumiChecker(object):
    """ 
    Simple utility class that helps correcting dataset that have lumis split across jobs:

    Due to error in processing (in particular, the Run I Tier-0), some
    lumi sections may be spread across multiple jobs. This class helps keep tracking of these
    lumis.
    """

    def __init__(self, applyLumiCorrection):
        # This is a dictionary that contains (run, lumis) pairs as keys, and job ojects as values
        # The run/lumi keys are added as soon as the lumi is processed by the splitting algorithm
        # The job value is added when the newJob method is invoked
        self.lumiJobs = {}
        # This dictionary contains (run, lumis) pairs as keys, and a list of files as values
        # The logic is that as soon as a split lumi is seen we add its input file here
        self.splitLumiFiles = {}
        self.applyLumiCorrection = applyLumiCorrection

    def isSplitLumi(self, run, lumi, file_):
        """ Check if a lumi has already been processed, and return True if it is the case.
            Also saves the input file containing the lumi if this happens.

            The method adds the (run, lumi) pair key to lumiJobs, and it sets its value to None.
            This value will be set from None to the job object as soon as the splitting algorithm
            switch to a new job.
            If a split lumi is encountered we add its input file to the self.splitLumiFiles dict
        """
        if not self.applyLumiCorrection:  # if we don't have to apply the correction simply exit
            return False

        # This means the lumi has already been processed and the job has changed
        isSplit = (run, lumi) in self.lumiJobs

        if isSplit:
            self.splitLumiFiles.setdefault((run, lumi), []).append(file_)
            logging.warning("Skipping runlumi pair (%s, %s) as it was already been processed."
                            "Will add %s to the input files of the job processing the lumi", run,
                            lumi, file_['lfn'])
        else:
            self.lumiJobs[(run, lumi)] = None

        return isSplit

    def closeJob(self, job):
        """ Go through the list of lumis of the job and add an entry to "lumiJobs"

            For each (run,lumi) pair in the job I create an entry in the dictionary so we know if the lumi
            has already been added to another job, and we know to which job (so later we can add files to this
            job if duplicated lumis are found)
        """
        if not self.applyLumiCorrection:
            return
        if job:  # the first time you call "newJob" in the splitting algorithm currentJob is None
            for run, lumiIntervals in viewitems(job['mask']['runAndLumis']):
                for startLumi, endLumi in lumiIntervals:
                    for lumi in range(startLumi, endLumi + 1):
                        self.lumiJobs[(run, lumi)] = job

    def fixInputFiles(self):
        """ Called at the end. Iterates over the split lumis, and add their input files to the first job where the lumi
            was seen.
        """
        # Just a cosmetic "if": self.splitLumiFiles is empty when applyLumiCorrection is not enabled
        if not self.applyLumiCorrection:
            return

        for (run, lumi), files in viewitems(self.splitLumiFiles):
            for file_ in files:
                self.lumiJobs[(run, lumi)].addFile(file_)


class LumiBased(JobFactory):
    """
    Split jobs by number of events
    """

    locations = []

    def __init__(self, package='WMCore.DataStructs', subscription=None, generators=None, limit=0):
        super(LumiBased, self).__init__(package, subscription, generators, limit)

        self.loadRunLumi = None  # Placeholder for DAO factory if needed
        self.collectionName = None  # Placeholder for ACDC Collection Name, if needed

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Split files into a number of lumis per job
        Allow a flag to determine if we split files between jobs
        """

        lumisPerJob = int(kwargs.get('lumis_per_job', 1))
        totalLumis = int(kwargs.get('total_lumis', 0))
        splitOnFile = bool(kwargs.get('halt_job_on_file_boundaries', False))
        self.collectionName = kwargs.get('collectionName', None)
        splitOnRun = kwargs.get('splitOnRun', True)
        getParents = kwargs.get('include_parents', False)
        runWhitelist = kwargs.get('runWhitelist', [])
        runs = kwargs.get('runs', None)
        lumis = kwargs.get('lumis', None)
        deterministicPileup = kwargs.get('deterministicPileup', False)
        applyLumiCorrection = bool(kwargs.get('applyLumiCorrection', False))
        eventsPerLumiInDataset = 0

        if lumisPerJob <= 0:
            msg = "lumis_per_job parameter must be positive. Its value is: %d" % lumisPerJob
            raise RuntimeError(msg)

        if self.package == 'WMCore.WMBS':
            self.loadRunLumi = self.daoFactory(classname="Files.GetBulkRunLumi")
            if deterministicPileup:
                getJobNumber = self.daoFactory(classname="Jobs.GetNumberOfJobsPerWorkflow")
                self.nJobs = getJobNumber.execute(workflow=self.subscription.getWorkflow().id)
                logging.info('Creating jobs in DeterministicPileup mode for %s',
                             self.subscription.workflowName())

        timePerEvent, sizePerEvent, memoryRequirement = \
            self.getPerformanceParameters(kwargs.get('performance', {}))

        goodRunList = {}
        if runs and lumis:
            goodRunList = buildLumiMask(runs, lumis)

        # If we have runLumi info, we need to load it from couch
        if self.collectionName:
            try:
                from WMCore.ACDC.DataCollectionService import DataCollectionService
                couchURL = kwargs.get('couchURL')
                couchDB = kwargs.get('couchDB')
                filesetName = kwargs.get('filesetName')

                logging.info('Creating jobs for ACDC fileset %s', filesetName)
                dcs = DataCollectionService(couchURL, couchDB)
                goodRunList = dcs.getLumiWhitelist(self.collectionName, filesetName)
            except Exception as ex:
                msg = "Exception while trying to load goodRunList. "
                msg += "Refusing to create any jobs.\nDetails: %s" % str(ex)
                logging.exception(msg)
                return

        lDict = self.getFilesSortedByLocation(lumisPerJob)
        if not lDict:
            logging.info("There are not enough lumis/files to be splitted. Trying again next cycle")
            return
        locationDict = {}
        for key in lDict:
            newlist = []
            for f in lDict[key]:
                # if hasattr(f, 'loadData'):
                #    f.loadData()
                if len(f['runs']) == 0:
                    continue
                f['lumiCount'] = 0
                f['runs'] = sorted(f['runs'])
                for run in f['runs']:
                    run.lumis.sort()
                    f['lumiCount'] += len(run.lumis)
                f['lowestRun'] = f['runs'][0]
                # Do average event per lumi calculation
                if f['lumiCount']:
                    f['avgEvtsPerLumi'] = int(round(f['events'] / f['lumiCount']))
                    if deterministicPileup:
                        # We assume that all lumis are equal in the dataset
                        eventsPerLumiInDataset = f['avgEvtsPerLumi']
                else:
                    # No lumis in the file, ignore it
                    continue
                newlist.append(f)
            locationDict[key] = sorted(newlist, key=operator.itemgetter('lowestRun'))

        # Split files into jobs with each job containing
        # EXACTLY lumisPerJob number of lumis (except for maybe the last one)

        totalJobs = 0
        lastLumi = None
        firstLumi = None
        stopJob = True
        stopTask = False
        lastRun = None
        lumisInJob = 0
        lumisInTask = 0
        self.lumiChecker = LumiChecker(applyLumiCorrection)
        for location in locationDict:

            # For each location, we need a new jobGroup
            self.newGroup()
            stopJob = True
            for f in locationDict[location]:
                if getParents:
                    parentLFNs = self.findParent(lfn=f['lfn'])
                    for lfn in parentLFNs:
                        parent = File(lfn=lfn)
                        f['parents'].add(parent)

                if splitOnFile:
                    # Then we have to split on every boundary
                    stopJob = True

                for run in f['runs']:
                    if not isGoodRun(goodRunList=goodRunList, run=run.run):
                        # Then skip this one
                        continue
                    if len(runWhitelist) > 0 and not run.run in runWhitelist:
                        # Skip due to run whitelist
                        continue
                    firstLumi = None

                    if splitOnRun and run.run != lastRun:
                        # Then we need to kill this job and get a new one
                        stopJob = True

                    # Now loop over the lumis
                    for lumi in run:
                        # splitLumi checks if the lumi is split across jobs
                        if (not isGoodLumi(goodRunList, run=run.run, lumi=lumi)
                            or self.lumiChecker.isSplitLumi(run.run, lumi, f)):
                            # Kill the chain of good lumis
                            # Skip this lumi
                            if firstLumi != None and firstLumi != lumi:
                                self.currentJob['mask'].addRunAndLumis(run=run.run,
                                                                       lumis=[firstLumi, lastLumi])
                                addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                                runAddedTime = addedEvents * timePerEvent
                                runAddedSize = addedEvents * sizePerEvent
                                self.currentJob.addResourceEstimates(jobTime=runAddedTime,
                                                                     disk=runAddedSize)
                                firstLumi = None
                                lastLumi = None
                            continue

                        # You have to kill the lumi chain if they're not continuous
                        if lastLumi and not lumi == lastLumi + 1:
                            self.currentJob['mask'].addRunAndLumis(run=run.run,
                                                                   lumis=[firstLumi, lastLumi])
                            addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                            runAddedTime = addedEvents * timePerEvent
                            runAddedSize = addedEvents * sizePerEvent
                            self.currentJob.addResourceEstimates(jobTime=runAddedTime,
                                                                 disk=runAddedSize)
                            firstLumi = None
                            lastLumi = None

                        if firstLumi is None:
                            # Set the first lumi in the run
                            firstLumi = lumi

                        # If we're full, end the job
                        if lumisInJob == lumisPerJob:
                            stopJob = True
                        # Actually do the new job creation
                        if stopJob:
                            if firstLumi != None and lastLumi != None and lastRun != None:
                                self.currentJob['mask'].addRunAndLumis(run=lastRun,
                                                                       lumis=[firstLumi, lastLumi])
                                addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                                runAddedTime = addedEvents * timePerEvent
                                runAddedSize = addedEvents * sizePerEvent
                                self.currentJob.addResourceEstimates(jobTime=runAddedTime,
                                                                     disk=runAddedSize)
                            # before creating a new job add the lumis of the current one to the checker
                            self.lumiChecker.closeJob(self.currentJob)
                            self.newJob(name=self.getJobName())
                            self.currentJob.addResourceEstimates(memory=memoryRequirement)
                            if deterministicPileup:
                                skipEvents = (self.nJobs - 1) * lumisPerJob * eventsPerLumiInDataset
                                self.currentJob.addBaggageParameter("skipPileupEvents", skipEvents)
                            firstLumi = lumi
                            lumisInJob = 0
                            totalJobs += 1

                            # Add the file to new jobs
                            self.currentJob.addFile(f)

                        lumisInJob += 1
                        lumisInTask += 1
                        lastLumi = lumi
                        stopJob = False
                        lastRun = run.run

                        if self.currentJob and not f in self.currentJob['input_files']:
                            self.currentJob.addFile(f)

                        if totalLumis > 0 and lumisInTask >= totalLumis:
                            stopTask = True
                            break

                    if firstLumi != None and lastLumi != None:
                        # Add this run to the mask
                        self.currentJob['mask'].addRunAndLumis(run=run.run,
                                                               lumis=[firstLumi, lastLumi])
                        addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                        runAddedTime = addedEvents * timePerEvent
                        runAddedSize = addedEvents * sizePerEvent
                        self.currentJob.addResourceEstimates(jobTime=runAddedTime, disk=runAddedSize)
                        firstLumi = None
                        lastLumi = None

                    if stopTask:
                        break

                if stopTask:
                    break

            if stopTask:
                break

        self.lumiChecker.closeJob(self.currentJob)
        self.lumiChecker.fixInputFiles()
        return

    def getFilesSortedByLocation(self, lumisPerJob):
        """
        _getFilesSortedByLocation_

        Retrieves a list of files available and sort them by location.
        If the fileset is closed, resume the splitting. Otherwise check whether
        there are enough lumis in each of these locations. If lumis don't
        match the desired lumis_per_job splitting parameter, then skip those
        files until further cycles.
        :param lumisPerJob: number of lumi sections desired in the splitting
        :return: a dictionary of files, key'ed by a frozenset location
        """
        lDict = self.sortByLocation()
        if not self.loadRunLumi:
            return lDict  # then it's a DataStruct/CRAB splitting

        checkMinimumWork = self.checkForAmountOfWork()

        # first, check whether we have enough files to reach the desired lumis_per_job
        for sites in list(lDict):
            fileLumis = self.loadRunLumi.execute(files=lDict[sites])
            if not fileLumis:
                logging.warning("Empty fileLumis dict for workflow %s, subs %s.",
                                self.subscription.workflowName(), self.subscription['id'])
            if checkMinimumWork:
                # fileLumis has a format like {230: {1: [1]}, 232: {1: [2]}, 304: {1: [3]}, 306: {1: [4]}}
                availableLumisPerLocation = [runL for fileItem in viewvalues(fileLumis) for runL in viewvalues(fileItem)]

                if lumisPerJob > len(flattenList(availableLumisPerLocation)):
                    # then we don't split these files for the moment
                    lDict.pop(sites)
                    continue
            for f in lDict[sites]:
                lumiDict = fileLumis.get(f['id'], {})
                for run in lumiDict:
                    f.addRun(run=Run(run, *lumiDict[run]))

        return lDict
