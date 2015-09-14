#!/usr/bin/env python
# pylint: disable=W0613
"""
_LumiBased_

Lumi based splitting algorithm that will chop a fileset into
a set of jobs based on lumi sections
"""




import operator
import logging
import threading
import traceback

from WMCore.DataStructs.Run import Run

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File               import File
from WMCore.WMSpec.WMTask           import buildLumiMask

def isGoodLumi(goodRunList, run, lumi):
    """
    _isGoodLumi_

    Checks to see if runs match a run-lumi combination in the goodRunList
    This is a pain in the ass.
    """
    if goodRunList == None or goodRunList == {}:
        return True

    if not isGoodRun(goodRunList = goodRunList, run = run):
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
    if goodRunList == None or goodRunList == {}:
        return True

    if str(run) in goodRunList.keys():
        # @e can find a run
        return True

    return False

class LumiChecker:
    """ Simple utility class that helps correcting dataset that have lumis split across jobs:

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
        if not self.applyLumiCorrection: # if we don't have to apply the correction simply exit
            return False

        # This means the lumi has already been processed and the job has changed
        isSplit = (run, lumi) in self.lumiJobs

        if isSplit:
            self.splitLumiFiles.setdefault((run, lumi), []).append(file_)
            logging.warning("Skipping runlumi pair (%s, %s) as it was already been processed."
                            "Will add %s to the input files of the job processing the lumi"
                                    % (run, lumi, file_['lfn']))
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
        if job: # the first time you call "newJob" in the splitting algorithm currentJob is None
            for run, lumiIntervals in job['mask']['runAndLumis'].iteritems():
                for startLumi, endLumi in lumiIntervals:
                    for lumi in xrange(startLumi, endLumi + 1):
                        self.lumiJobs[(run, lumi)] = job

    def fixInputFiles(self):
        """ Called at the end. Iterates over the split lumis, and add their input files to the first job where the lumi
            was seen.
        """
        # Just a cosmetic "if": self.splitLumiFiles is empty when applyLumiCorrection is not enabled
        if not self.applyLumiCorrection:
            return

        for (run, lumi), files in self.splitLumiFiles.iteritems():
            for file_ in files:
                self.lumiJobs[(run, lumi)].addFile(file_)



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

        myThread = threading.currentThread()

        lumisPerJob = int(kwargs.get('lumis_per_job', 1))
        totalLumis = int(kwargs.get('total_lumis', 0))
        splitOnFile = bool(kwargs.get('halt_job_on_file_boundaries', True))
        ignoreACDC = bool(kwargs.get('ignore_acdc_except', False))
        collectionName = kwargs.get('collectionName', None)
        splitOnRun = kwargs.get('splitOnRun', True)
        getParents = kwargs.get('include_parents', False)
        runWhitelist = kwargs.get('runWhitelist', [])
        runs = kwargs.get('runs', None)
        lumis = kwargs.get('lumis', None)
        deterministicPileup = kwargs.get('deterministicPileup', False)
        applyLumiCorrection = bool(kwargs.get('applyLumiCorrection', False))
        eventsPerLumiInDataset = 0

        if deterministicPileup and self.package == 'WMCore.WMBS':
            getJobNumber = self.daoFactory(classname = "Jobs.GetNumberOfJobsPerWorkflow")
            jobNumber = getJobNumber.execute(workflow = self.subscription.getWorkflow().id)
            self.nJobs = jobNumber

        timePerEvent, sizePerEvent, memoryRequirement = \
                    self.getPerformanceParameters(kwargs.get('performance', {}))

        goodRunList = {}
        if runs and lumis:
            goodRunList = buildLumiMask(runs, lumis)

        # If we have runLumi info, we need to load it from couch
        if collectionName:
            try:
                from WMCore.ACDC.DataCollectionService import DataCollectionService
                couchURL = kwargs.get('couchURL')
                couchDB = kwargs.get('couchDB')
                filesetName = kwargs.get('filesetName')
                collectionName = kwargs.get('collectionName')
                owner = kwargs.get('owner')
                group = kwargs.get('group')

                logging.info('Creating jobs for ACDC fileset %s' % filesetName)
                dcs = DataCollectionService(couchURL, couchDB)
                goodRunList = dcs.getLumiWhitelist(collectionName, filesetName, owner, group)
            except Exception as ex:
                msg = "Exception while trying to load goodRunList\n"
                if ignoreACDC:
                    msg += "Ditching goodRunList\n"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    logging.error(msg)
                    goodRunList = {}
                else:
                    msg += "Refusing to create any jobs.\n"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    logging.error(msg)
                    return

        lDict = self.sortByLocation()
        locationDict = {}

        # First we need to load the data
        if self.package == 'WMCore.WMBS':
            loadRunLumi = self.daoFactory(classname = "Files.GetBulkRunLumi")

        for key in lDict.keys():
            newlist = []
            # First we need to load the data
            if self.package == 'WMCore.WMBS':
                fileLumis = loadRunLumi.execute(files = lDict[key])
                for f in lDict[key]:
                    lumiDict = fileLumis.get(f['id'], {})
                    for run in lumiDict.keys():
                        f.addRun(run = Run(run, *lumiDict[run]))

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
                    f['avgEvtsPerLumi'] = round(float(f['events']) / f['lumiCount'])
                    if deterministicPileup:
                        # We assume that all lumis are equal in the dataset
                        eventsPerLumiInDataset = f['avgEvtsPerLumi']
                else:
                    # No lumis in the file, ignore it
                    continue
                newlist.append(f)
            locationDict[key] = sorted(newlist, key = operator.itemgetter('lowestRun'))

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
        for location in locationDict.keys():

            # For each location, we need a new jobGroup
            self.newGroup()
            stopJob = True
            for f in locationDict[location]:
                if getParents:
                    parentLFNs = self.findParent(lfn = f['lfn'])
                    for lfn in parentLFNs:
                        parent = File(lfn = lfn)
                        f['parents'].add(parent)

                if splitOnFile:
                    # Then we have to split on every boundary
                    stopJob = True

                for run in f['runs']:
                    if not isGoodRun(goodRunList = goodRunList, run = run.run):
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
                        if (not isGoodLumi(goodRunList, run = run.run, lumi = lumi)
                                or self.lumiChecker.isSplitLumi(run.run, lumi, f)): # splitLumi checks if the lumi is split across jobs
                            # Kill the chain of good lumis
                            # Skip this lumi
                            if firstLumi != None and firstLumi != lumi:
                                self.currentJob['mask'].addRunAndLumis(run = run.run,
                                                                       lumis = [firstLumi, lastLumi])
                                addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                                runAddedTime = addedEvents * timePerEvent
                                runAddedSize = addedEvents * sizePerEvent
                                self.currentJob.addResourceEstimates(jobTime = runAddedTime,
                                                                     disk = runAddedSize)
                                firstLumi = None
                                lastLumi = None
                            continue

                        # You have to kill the lumi chain if they're not continuous
                        if lastLumi and not lumi == lastLumi + 1:
                            self.currentJob['mask'].addRunAndLumis(run = run.run,
                                                                   lumis = [firstLumi, lastLumi])
                            addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                            runAddedTime = addedEvents * timePerEvent
                            runAddedSize = addedEvents * sizePerEvent
                            self.currentJob.addResourceEstimates(jobTime = runAddedTime,
                                                                 disk = runAddedSize)
                            firstLumi = None
                            lastLumi = None

                        if firstLumi == None:
                            # Set the first lumi in the run
                            firstLumi = lumi

                        # If we're full, end the job
                        if lumisInJob == lumisPerJob:
                            stopJob = True
                        # Actually do the new job creation
                        if stopJob:
                            if firstLumi != None and lastLumi != None and lastRun != None:
                                self.currentJob['mask'].addRunAndLumis(run = lastRun,
                                                                       lumis = [firstLumi, lastLumi])
                                addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                                runAddedTime = addedEvents * timePerEvent
                                runAddedSize = addedEvents * sizePerEvent
                                self.currentJob.addResourceEstimates(jobTime = runAddedTime,
                                                                     disk = runAddedSize)
                            self.lumiChecker.closeJob(self.currentJob) # before creating a new job add the lumis of the current one to the checker
                            self.newJob(name = self.getJobName())
                            self.currentJob.addResourceEstimates(memory = memoryRequirement)
                            if deterministicPileup:
                                self.currentJob.addBaggageParameter("skipPileupEvents", (self.nJobs - 1) * lumisPerJob * eventsPerLumiInDataset)
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
                        self.currentJob['mask'].addRunAndLumis(run = run.run,
                                                               lumis = [firstLumi, lastLumi])
                        addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                        runAddedTime = addedEvents * timePerEvent
                        runAddedSize = addedEvents * sizePerEvent
                        self.currentJob.addResourceEstimates(jobTime = runAddedTime, disk = runAddedSize)
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
